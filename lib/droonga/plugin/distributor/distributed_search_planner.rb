# -*- coding: utf-8 -*-
#
# Copyright (C) 2013-2014 Droonga Project
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1 as published by the Free Software Foundation.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

module Droonga
  class DistributedSearchPlanner < DistributedCommandPlanner
    def initialize(search_request_message)
      super

      @request = @source_message["body"]
      @queries = @request["queries"]

      @query_outputs = []
    end

    def build_messages
      Searcher::QuerySorter.validate_dependencies(@queries)

      ensure_unifiable!

      @queries.each do |input_name, query|
        transform_query(input_name, query)
      end

      @dataset = @source_message["dataset"] || @request["dataset"]
      broadcast_at_random(@request)
    end

    private
    UNLIMITED = -1

    def ensure_unifiable!
      @queries.each do |name, query|
        if unifiable?(name) && query["output"]
          query["output"]["unifiable"] = true
        end
      end
    end

    def unifiable?(name)
      query = @queries[name]
      return true if query["groupBy"]
      name = query["source"]
      return false unless @queries.keys.include?(name)
      unifiable?(name)
    end

    def transform_query(input_name, query)
      output = query["output"]

      # Skip reducing phase for a result with no output.
      if output.nil? or
           output["elements"].nil? or
           (!output["elements"].include?("count") &&
            !output["elements"].include?("records"))
        return
      end

      @query_outputs << output_name(input_name)

      transformer = QueryTransformer.new(query)

      @reducers << reducer_message("search_reduce",
                                   input_name,
                                   transformer.reducers)
      @gatherers << gatherer_message("search_gather",
                                     input_name,
                                     "elements" => transformer.mappers)
    end

    class QueryTransformer
      attr_reader :reducers, :mappers

      def initialize(query)
        @query = query
        @output = @query["output"]
        @reducers = {}
        @mappers = {}
        @output_records = true
        transform!
      end

      def transform!
        # The collector module supports only "simple" format search results.
        # So we have to override the format and restore it on the gathering
        # phase.
        @records_format = @output["format"] || "simple"
        @output["format"] = "simple"

        @sort_keys = @query["sortBy"] || []
        @sort_keys = @sort_keys["keys"] || [] if @sort_keys.is_a?(Hash)

        calculate_offset_and_limit!
        build_count_mapper_and_reducer!
        build_records_mapper_and_reducer!
      end

      def calculate_offset_and_limit!
        @original_sort_offset = sort_offset
        @original_output_offset = output_offset
        @original_sort_limit = sort_limit
        @original_output_limit = output_limit

        calculate_sort_offset!
        calculate_output_offset!

        # We have to calculate limit based on offset.
        # <A, B = limited integer (0...MAXINT)>
        # | sort limit | output limit | => | worker's sort limit      | worker's output limit   | final limit |
        # =============================    ====================================================================
        # | UNLIMITED  | UNLIMITED    | => | UNLIMITED                | UNLIMITED               | UNLIMITED   |
        # | UNLIMITED  | B            | => | final_offset + B         | final_offset + B        | B           |
        # | A          | UNLIMITED    | => | final_offset + A         | final_offset + A        | A           |
        # | A          | B            | => | final_offset + max(A, B) | final_offset + min(A, B)| min(A, B)   |

        # XXX final_limit and final_offset calculated in many times

        @records_offset = final_offset
        @records_limit = final_limit

        if final_limit == UNLIMITED
          @output["limit"] = UNLIMITED
        else
          if rich_sort?
            @query["sortBy"]["limit"] = final_offset + [sort_limit, output_limit].max
          end
          @output["limit"] = final_offset + final_limit
        end
      end

      def calculate_sort_offset!
        # Offset for workers must be zero, because we have to apply "limit" and
        # "offset" on the last gathering phase instead of each reducing phase.
        if rich_sort?
          @query["sortBy"]["offset"] = 0
        end
      end

      def sort_offset
        if rich_sort?
          @query["sortBy"]["offset"] || 0
        else
          0
        end
      end

      def output_offset
        @output["offset"] || 0
      end

      def sort_limit
        if rich_sort?
          @query["sortBy"]["limit"] || UNLIMITED
        else
          UNLIMITED
        end
      end

      def output_limit
        @output["limit"] || 0
      end

      def calculate_output_offset!
        @output["offset"] = 0 if have_records?
      end

      def final_offset
        @original_sort_offset + @original_output_offset
      end

      def final_limit
        if @original_sort_limit == UNLIMITED && @original_output_limit == UNLIMITED
          UNLIMITED
        else
          if @original_sort_limit == UNLIMITED
            @original_output_limit
          elsif @original_output_limit == UNLIMITED
            @original_sort_limit
          else
            [@original_sort_limit, @original_output_limit].min
          end
        end
      end

      def have_records?
        @output["elements"].include?("records")
      end

      def rich_sort?
        @query["sortBy"].is_a?(Hash)
      end

      def unifiable?
        @output["unifiable"]
      end

      def build_count_mapper_and_reducer!
        return unless @output["elements"].include?("count")

        @reducers["count"] = {
          "type" => "sum",
        }
        if unifiable?
          @query["sortBy"]["limit"] = -1 if @query["sortBy"].is_a?(Hash)
          @output["limit"] = -1
          mapper = {
            "type" => "count",
            "target" => "records",
          }
          unless @output["elements"].include?("records")
            @records_limit = -1
            @output["elements"] << "records"
            @output["attributes"] ||= ["_key"]
            @output_records = false
          end
          @mappers["count"] = mapper
        end
      end

      def build_records_mapper_and_reducer!
        # Skip reducing phase for a result with no record output.
        return if !@output["elements"].include?("records") || @records_limit.zero?

        # Append sort key attributes to the list of output attributes
        # temporarily, for the reducing phase. After all extra columns
        # are removed on the gathering phase.
        final_attributes = output_attribute_names
        update_output_attributes!

        @reducers["records"] = build_records_reducer

        mapper = {
          "type" => "sort",
          "offset" => @records_offset,
          "limit" => @records_limit,
          "format" => @records_format,
          "attributes" => final_attributes,
        }
        mapper["no_output"] = true unless @output_records
        @mappers["records"] = mapper
      end

      def output_attribute_names
        attributes = @output["attributes"] || []
        if attributes.is_a?(Hash)
          attributes.keys
        else
          attributes.collect do |attribute|
            if attribute.is_a?(Hash)
              attribute["label"] || attribute["source"]
            else
              attribute
            end
          end
        end
      end

      def update_output_attributes!
        @output["attributes"] = array_style_attributes
        @output["attributes"] += sort_attribute_names
        if unifiable? && !@output["attributes"].include?("_key")
          @output["attributes"] << "_key"
        end
      end

      def array_style_attributes
        attributes = @output["attributes"] || []
        if attributes.is_a?(Hash)
          attributes.keys.collect do |key|
            attribute = attributes[key]
            case attribute
            when String
              {
                "label" => key,
                "source" => attribute,
              }
            when Hash
              attribute["label"] = key
              attribute
            end
          end
        else
          attributes
        end
      end

      def source_column_names
        attributes = @output["attributes"] || []
        if attributes.is_a?(Hash)
          attributes_hash = attributes
          attributes = []
          attributes_hash.each do |key, attribute|
            attributes << attribute["source"] || key
          end
          attributes
        else
          attributes.collect do |attribute|
            if attribute.is_a?(Hash)
              attribute["source"] || attribute["label"]
            else
              attribute
            end
          end
        end
      end

      def sort_attribute_names
        sort_attributes = @sort_keys.collect do |key|
          key = key[1..-1] if key[0] == "-"
          key
        end
        attributes = source_column_names
        sort_attributes.reject! do |attribute|
          attributes.include?(attribute)
        end
        sort_attributes
      end

      ASCENDING_OPERATOR = "<"
      DESCENDING_OPERATOR = ">"

      def build_records_reducer
        attributes = @output["attributes"]
        key_column_index = attributes.index("_key")

        operators = @sort_keys.collect do |sort_key|
          operator = ASCENDING_OPERATOR
          if sort_key[0] == "-"
            operator = DESCENDING_OPERATOR
            sort_key = sort_key[1..-1]
          end
          {
            "operator" => operator,
            "column" => attributes.index(sort_key),
          }
        end

        reducer = {
          "type" => "sort",
          "operators" => operators,
        }
        if unifiable? && !key_column_index.nil?
          reducer["key_column"] = key_column_index
        end

        # On the reducing phase, we apply only "limit". We cannot apply
        # "offset" on this phase because the collector merges a pair of
        # results step by step even if there are three or more results.
        # Instead, we apply "offset" on the gathering phase.
        reducer["limit"] = @output["limit"]

        reducer
      end
    end
  end
end
