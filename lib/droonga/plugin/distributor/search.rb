# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Droonga Project
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

require "droonga/distributor_plugin"

module Droonga
  class SearchDistributor < Droonga::DistributorPlugin
    repository.register("search", self)

    command :search
    def search(envelope)
      message = []
      input_names = []
      output_names = []
      output_mapper = {}

      request = envelope["body"]
      queries = request["queries"]

      queries.each do |input_name, query|
        if query["groupBy"] && query["output"]
          query["output"]["canUnify"] = true
        end
      end

      queries.each do |input_name, query|
        output = query["output"]
        # Skip reducing phase for a result with no output.
        next unless output

        input_names << input_name
        output_name = input_name + "_reduced"
        output_names << output_name
        output_mapper[output_name] = {
          "output" => input_name,
        }

        # The collector module supports only "simple" format search results.
        # So we have to override the format and restore it on the gathering
        # phase.
        final_format = output["format"] || "simple"
        output["format"] = "simple"

        final_offset, final_limit = calculate_offset_and_limit!(query)

        elements = {}
        output["elements"].each do |element|
          case element
          when "count"
            elements[element] = {
              "type" => "sum",
            }
          when "records"
            # Skip reducing phase for a result with no record output.
            next if final_limit.zero?

            # Append sort key attributes to the list of output attributes
            # temporarily, for the reducing phase. After all extra columns
            # are removed on the gathering phase.
            final_attributes = collect_output_attributes(output["attributes"])
            output["attributes"] = format_attributes_to_array_style(output["attributes"])
            output["attributes"] += collect_sort_attributes(output["attributes"], query["sortBy"])
            unify_by_key = output["canUnify"]
            if unify_by_key && !output["attributes"].include?("_key")
              output["attributes"] << "_key"
            end
 
            elements[element] = sort_reducer(:attributes => output["attributes"],
                                             :sort_keys => query["sortBy"],
                                             :unify_by_key => unify_by_key)
            # On the reducing phase, we apply only "limit". We cannot apply
            # "offset" on this phase because the collecter merges a pair of
            # results step by step even if there are three or more results.
            # Instead, we apply "offset" on the gethering phase.
            elements[element]["limit"] = output["limit"]

            output_mapper[output_name]["element"] = element
            output_mapper[output_name]["offset"] = final_offset
            output_mapper[output_name]["limit"] = final_limit
            output_mapper[output_name]["format"] = final_format
            output_mapper[output_name]["attributes"] = final_attributes
          end
        end

        reducer = {
          "type" => "reduce",
          "body" => {
            input_name => {
              output_name => elements,
            },
          },
          "inputs" => [input_name], # XXX should be placed in the "body"?
          "outputs" => [output_name], # XXX should be placed in the "body"?
        }
        message << reducer
      end
      gatherer = {
        "type" => "gather",
        "body" => output_mapper,
        "inputs" => output_names, # XXX should be placed in the "body"?
        "post" => true, # XXX should be placed in the "body"?
      }
      message << gatherer
      searcher = {
        "type" => "broadcast",
        "command" => "search", # XXX should be placed in the "body"?
        "dataset" => envelope["dataset"] || request["dataset"],
        "body" => request,
        "outputs" => input_names, # XXX should be placed in the "body"?
        "replica" => "random", # XXX should be placed in the "body"?
      }
      message.push(searcher)
      post(message)
    end

    private
    UNLIMITED = -1

    def calculate_offset_and_limit!(query)
      rich_sort = query["sortBy"].is_a?(Hash)

      have_records = false
      if query["output"] &&
           query["output"]["elements"].is_a?(Array) &&
           query["output"]["elements"].include?("records")
        have_records = true
      end

      # Offset for workers must be zero, because we have to apply "limit" and
      # "offset" on the last gapthering phase instaed of each reducing phase.
      sort_offset = 0
      if rich_sort
        sort_offset = query["sortBy"]["offset"] || 0
        query["sortBy"]["offset"] = 0
      end

      output_offset = query["output"]["offset"] || 0
      query["output"]["offset"] = 0 if have_records

      final_offset = sort_offset + output_offset

      # We have to calculate limit based on offset.
      # <A, B = limited integer (0...MAXINT)>
      # | sort limit | output limit | => | worker's sort limit      | worker's output limit   | final limit |
      # =============================    ====================================================================
      # | UNLIMITED  | UNLIMITED    | => | UNLIMITED                | UNLIMITED               | UNLIMITED   |
      # | UNLIMITED  | B            | => | final_offset + B         | final_offset + B        | B           |
      # | A          | UNLIMITED    | => | final_offset + A         | final_offset + A        | A           |
      # | A          | B            | => | final_offset + min(A, B) | final_offset + min(A, B)| min(A, B)   |
      sort_limit = UNLIMITED
      if rich_sort
        sort_limit = query["sortBy"]["limit"] || UNLIMITED
      end
      output_limit = query["output"]["limit"] || 0

      final_limit = 0
      if sort_limit == UNLIMITED && output_limit == UNLIMITED
        final_limit = UNLIMITED
        query["output"]["limit"] = UNLIMITED
      else
        if sort_limit == UNLIMITED
          final_limit = output_limit
        elsif output_limit == UNLIMITED
          final_limit = sort_limit
        else
          final_limit = [sort_limit, output_limit].min
        end
        query["sortBy"]["limit"] = final_offset + final_limit if rich_sort
        query["output"]["limit"] = final_offset + final_limit
      end

      [final_offset, final_limit]
    end

    def format_attributes_to_array_style(attributes)
      attributes ||= []
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

    def collect_output_attributes(attributes)
      attributes ||= []
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

    def collect_source_column_names(attributes)
      attributes ||= []
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

    def collect_sort_attributes(attributes, sort_keys)
      sort_keys ||= []
      sort_keys = sort_keys["keys"] || [] if sort_keys.is_a?(Hash)

      attributes = collect_source_column_names(attributes)

      sort_attributes = sort_keys.collect do |key|
        key = key[1..-1] if key[0] == "-"
        key
      end
      sort_attributes.reject! do |attribute|
        attributes.include?(attribute)
      end
      sort_attributes      
    end

    ASCENDING_OPERATOR = "<".freeze
    DESCENDING_OPERATOR = ">".freeze
    MERGE_ATTRIBUTES = ["_nsubrecs", "_subrecs"]

    def sort_reducer(params={})
      attributes = params[:attributes] || []
      sort_keys = params[:sort_keys] || []
      sort_keys = sort_keys["keys"] || [] if sort_keys.is_a?(Hash)

      key_column_index = attributes.index("_key")
      unified_columns = []
      attributes.each_with_index do |attribute, index|
        source = attribute
        source = attribute["source"] if attribute.is_a?(Hash)
        unified_columns << index if MERGE_ATTRIBUTES.include?(source)
      end

      operators = sort_keys.collect do |sort_key|
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
      if params[:unify_by_key] && !key_column_index.nil?
        reducer["key_column"] = key_column_index
        reducer["unified_columns"] = unified_columns unless unified_columns.empty?
      end
      reducer
    end
  end
end
