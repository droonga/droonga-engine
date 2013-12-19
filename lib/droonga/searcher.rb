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

require "English"
require "tsort"
require "groonga"

module Droonga
  class Searcher
    class Error < StandardError
    end

    class UndefinedSourceError < Error
      attr_reader :name
      def initialize(name)
        @name = name
        super("undefined source was used: <#{name}>")
      end
    end

    def initialize(context)
      @context = context
    end

    def search(queries)
      outputs = nil
      $log.trace("#{log_tag}: search: start", :queries => queries)
      @context.push_memory_pool do
        outputs = process_queries(queries)
      end
      $log.trace("#{log_tag}: search: done")
      return outputs
    end

    private
    def process_queries(queries)
      $log.trace("#{log_tag}: process_queries: start")
      unless queries
        $log.trace("#{log_tag}: process_queries: done")
        return {}
      end
      $log.trace("#{log_tag}: process_queries: sort: start")
      query_sorter = QuerySorter.new
      queries.each do |name, query|
        query_sorter.add(name, [query["source"]])
      end
      sorted_queries = query_sorter.tsort
      $log.trace("#{log_tag}: process_queries: sort: done")
      outputs = {}
      results = {}
      sorted_queries.each do |name|
        if queries[name]
          $log.trace("#{log_tag}: process_queries: search: start",
                     :name => name)
          search_request = SearchRequest.new(@context, queries[name])
          searcher = QuerySearcher.new(search_request)
          search_result = searcher.search(results)
          results[name] = search_result.records
          $log.trace("#{log_tag}: process_queries: search: done",
                     :name => name)
          if searcher.need_output?
            $log.trace("#{log_tag}: process_queries: format: start",
                       :name => name)
            outputs[name] = searcher.format
            $log.trace("#{log_tag}: process_queries: format: done",
                       :name => name)
          end
        elsif @context[name]
          results[name] = @context[name]
        else
          raise UndefinedSourceError.new(name)
        end
      end
      $log.trace("#{log_tag}: process_queries: done")
      return outputs
    end

    def log_tag
      "[#{Process.ppid}][#{Process.pid}] searcher"
    end

    class QuerySorter
      include TSort
      def initialize()
        @queries = {}
      end

      def add(name, sources=[])
        @queries[name] = sources
      end

      def tsort_each_node(&block)
        @queries.each_key(&block)
      end

      def tsort_each_child(node, &block)
        if @queries[node]
          @queries[node].each(&block)
        end
      end
    end

    class ResultFormatter
      def initialize(search_request, search_result)
        @request = search_request
        @context = @request.context
        @query = @request.query

        @result = search_result
        @records = @result.records
        @start_time = @result.start_time
        @count = @result.count
        @condition = @result.condition
      end

      def format
        formatted_result = {}
        format_count(formatted_result)
        format_attributes(formatted_result)
        format_records(formatted_result)
        if need_element_output?("startTime")
          formatted_result["startTime"] = @start_time.iso8601
        end
        if need_element_output?("elapsedTime")
          formatted_result["elapsedTime"] = Time.now.to_f - @start_time.to_f
        end
        formatted_result
      end

      private
      def need_element_output?(element)
        params = @query["output"]

        elements = params["elements"]
        return false if elements.nil?

        elements.include?(element)
      end

      def format_count(formatted_result)
        return unless need_element_output?("count")
        formatted_result["count"] = @count
      end

      def format_attributes(formatted_result)
        return unless need_element_output?("attributes")

        # XXX IMPLEMENT ME!!!
        attributes = nil
        if @query["output"]["format"] == "complex"
          # should convert columns to an object like:
          # {"_id" => {"type" => "UInt32", "vector" => false}}
          attributes = {}
        else
          # should convert columns to an object like:
          # [{"name" => "_id", "type" => "UInt32", "vector" => false}]
          attributes = []
        end

        formatted_result["attributes"] = attributes
      end

      def format_records(formatted_result)
        return unless need_element_output?("records")

        params = @query["output"]

        attributes = params["attributes"]
        target_attributes = normalize_target_attributes(attributes)
        offset = params["offset"] || 0
        limit = params["limit"] || 10
        @records.open_cursor(:offset => offset, :limit => limit) do |cursor|
          if params["format"] == "complex"
            formatted_result["records"] = cursor.collect do |record|
              complex_record(target_attributes, record)
            end
          else
            formatted_result["records"] = cursor.collect do |record|
              simple_record(target_attributes, record)
            end
          end
        end
      end

      def complex_record(attributes, record)
        values = {}
        attributes.collect do |attribute|
          values[attribute[:label]] = record_value(record, attribute)
        end
        values
      end

      def simple_record(attributes, record)
        attributes.collect do |attribute|
          record_value(record, attribute)
        end
      end

      def record_value(record, attribute)
        if attribute[:source] == "_subrecs"
          if @query["output"]["format"] == "complex"
            record.sub_records.collect do |sub_record|
              target_attributes = resolve_attributes(attribute, sub_record)
              complex_record(target_attributes, sub_record)
            end
          else
            record.sub_records.collect do |sub_record|
              target_attributes = resolve_attributes(attribute, sub_record)
              simple_record(target_attributes, sub_record)
            end
          end
        else
          expression = attribute[:expression]
          if expression
            variable = attribute[:variable]
            variable.value = record
            expression.execute
          else
            value = record[attribute[:source]]
            if value.is_a?(Groonga::Record)
              value.record_id
            else
              value
            end
          end
        end
      end

      def resolve_attributes(attribute, record)
        unless attribute[:target_attributes]
          attribute[:target_attributes] =
            normalize_target_attributes(attribute[:attributes], record.table)
        end
        return attribute[:target_attributes]
      end

      def normalize_target_attributes(attributes, domain = @records)
        attributes.collect do |attribute|
          if attribute.is_a?(String)
            attribute = {
              "source" => attribute,
            }
          end
          source = attribute["source"]
          if accessor_name?(source)
            expression = nil
            variable = nil
          else
            expression = Groonga::Expression.new(context: @context)
            variable = expression.define_variable(domain: domain)
            expression.parse(source, syntax: :script)
            condition = expression.define_variable(name: "$condition",
                                                   reference: true)
            condition.value = @condition
            source = nil
          end
          {
            label: attribute["label"] || attribute["source"],
            source: source,
            expression: expression,
            variable: variable,
            attributes: attribute["attributes"]
          }
        end
      end

      def accessor_name?(source)
        /\A[a-zA-Z\#@$_][a-zA-Z\d\#@$_\-.]*\z/ === source
      end
    end

    class SearchRequest
      attr_reader :context, :query

      def initialize(context, query)
        @context = context
        @query = query
      end
    end

    class SearchResult
      attr_accessor :start_time, :condition, :records, :count

      def initialize
        @start_time = nil
        @condition = nil
        @records = nil
        @count = nil
      end
    end

    class QuerySearcher
      def initialize(search_request)
        @request = search_request
        @context = @request.context
        @query = @request.query
      end

      def search(results)
        @result = SearchResult.new
        search_query(results)
        @result
      end

      def need_output?
        @result.records and @query.has_key?("output")
      end

      def format
        formatter = ResultFormatter.new(@request, @result)
        formatter.format
      end

      private
      def parseCondition(source, expression, condition)
        if condition.is_a? String
          expression.parse(condition, :syntax => :script)
        elsif condition.is_a? Hash
          options = {}
          if condition["matchTo"]
            matchTo = Groonga::Expression.new(context: @context)
            matchTo.define_variable(:domain => source)
            match_columns = condition["matchTo"]
            match_columns = match_columns.join(",") if match_columns.is_a?(Array)
            matchTo.parse(match_columns, :syntax => :script)
            options[:default_column] = matchTo
          end
          if condition["query"]
            options[:syntax] = :query
            if condition["defaultOperator"]
              case condition["defaultOperator"]
              when "||"
                options[:default_operator] = Groonga::Operator::OR
              when "&&"
                options[:default_operator] = Groonga::Operator::AND
              when "-"
                options[:default_operator] = Groonga::Operator::BUT
              else
                raise "undefined operator assigned #{condition["default_operator"]}"
              end
            end
            if condition["allowPragma"]
              options[:allow_pragma] = true
            end
            if condition["allowColumn"]
              options[:allow_column] = true
            end
            expression.parse(condition["query"], options)
          elsif condition["script"]
            # "script" is ignored when "query" is also assigned.
            options[:syntax] = :script
            if condition["allowUpdate"]
              options[:allow_update] = true
            end
            expression.parse(condition["script"], options)
          else
            raise "neither 'query' nor 'script' assigned in #{condition.inspect}"
          end
        elsif condition.is_a? Array
          case condition[0]
          when "||"
            operator = Groonga::Operator::OR
          when "&&"
            operator = Groonga::Operator::AND
          when "-"
            operator = Groonga::Operator::BUT
          else
            raise "undefined operator assigned #{condition[0]}"
          end
          if condition[1]
            parseCondition(source, expression, condition[1])
          end
          condition[2..-1].each do |element|
            parseCondition(source, expression, element)
            expression.append_operation(operator, 2)
          end
        else
          raise "unacceptable object #{condition.inspect} assigned"
        end
      end

      def parse_order_keys(keys)
        keys.collect do |key|
          if key =~ /^-/
            [$POSTMATCH, :descending]
          else
            [key, :ascending]
          end
        end
      end

      def search_query(results)
        $log.trace("#{log_tag}: search_query: start")

        @result.start_time = Time.now
        @records = results[@query["source"]]

        condition = @query["condition"]
        apply_condition!(condition) if condition

        group_by = @query["groupBy"]
        apply_group_by!(group_by) if group_by

        @result.count = @records.size

        sort_by = @query["sortBy"]
        apply_sort_by!(sort_by) if sort_by

        $log.trace("#{log_tag}: search_query: done")
        @result.records = @records
      end

      def apply_condition!(condition)
        expression = Groonga::Expression.new(context: @context)
        expression.define_variable(:domain => @records)
        parseCondition(@records, expression, condition)
        $log.trace("#{log_tag}: search_query: select: start",
                   :condition => condition)
        @records = @records.select(expression)
        $log.trace("#{log_tag}: search_query: select: done")
        @result.condition = expression
      end

      def apply_group_by!(group_by)
        $log.trace("#{log_tag}: search_query: group: start",
                   :by => group_by)
        case group_by
        when String
          @records = @records.group(group_by)
        when Hash
          key = group_by["key"]
          max_n_sub_records = group_by["maxNSubRecords"]
          @records = @records.group(key, :max_n_sub_records => max_n_sub_records)
        else
          raise '"groupBy" parameter must be a Hash or a String'
        end
        $log.trace("#{log_tag}: search_query: group: done",
                   :by => group_by)
      end

      def apply_sort_by!(sort_by)
        $log.trace("#{log_tag}: search_query: sort: start",
                   :by => sort_by)
        case sort_by
        when Array
          keys = parse_order_keys(sort_by)
          offset = 0
          limit = -1
        when Hash
          keys = parse_order_keys(sort_by["keys"])
          offset = sort_by["offset"]
          limit = sort_by["limit"]
        else
          raise '"sortBy" parameter must be a Hash or an Array'
        end
        @records = @records.sort(keys, :offset => offset, :limit => limit)
        $log.trace("#{log_tag}: search_query: sort: done",
                   :by => sort_by)
      end

      def log_tag
        "[#{Process.ppid}][#{Process.pid}] query_searcher"
      end
    end
  end
end
