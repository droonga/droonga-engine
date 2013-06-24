# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 droonga project
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

require "droonga/handler"

module Droonga
  class SearchHandler < Droonga::Handler
    Droonga::HandlerPlugin.register("search", self)

    class Error < StandardError
    end

    class UndefinedSourceError < Error
      attr_reader :name
      def initialize(name)
        @name = name
        super("undefined source was used: <#{name}>")
      end
    end

    command :search
    def search(request)
      queries = request["queries"]
      outputs = process_queries(queries)
      post(outputs)
    end

    def process_queries(queries)
      return {} unless queries
      query_sorter = QuerySorter.new
      queries.each do |name, query|
        query_sorter.add(name, [query["source"]])
      end
      outputs = {}
      results = {}
      query_sorter.tsort.each do |name|
        if queries[name]
          searcher = QuerySearcher.new(@context, queries[name])
          results[name] = searcher.search(results)
          outputs[name] = searcher.format if searcher.need_output?
        elsif @context[name]
          results[name] = @context[name]
        else
          raise UndefinedSourceError.new(name)
        end
      end
      return outputs
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

    class QuerySearcher
      def initialize(context, query)
        @context = context
        @query = query
        @result = nil
        @condition = nil
        @start_time = nil
      end

      def search(results)
        search_query(results)
      end

      def need_output?
        @result and @query.has_key?("output")
      end

      def format
        formatted_result = {}
        format_count(formatted_result)
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
            if condition["default_operator"]
              case condition["default_operator"]
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
            if condition["allow_pragma"]
              options[:allow_pragma] = true
            end
            if condition["allow_column"]
              options[:allow_column] = true
            end
            expression.parse(condition["query"], options)
          elsif condition["script"]
            # "script" is ignored when "query" is also assigned.
            options[:syntax] = :script
            if condition["allow_update"]
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
        @start_time = Time.now
        @result = source = results[@query["source"]]
        if @query["condition"]
          expression = Groonga::Expression.new(context: @context)
          expression.define_variable(:domain => source)
          parseCondition(source, expression, @query["condition"])
          @result = source.select(expression)
          @condition = expression
        end
        if @query["groupBy"]
          if @query["groupBy"].is_a? String
            @result = @result.group(@query["groupBy"])
          elsif @query["groupBy"].is_a? Hash
            key = @query["groupBy"]["key"]
            max_n_sub_records = @query["groupBy"]["maxNSubRecords"]
            @result = @result.group(key, :max_n_sub_records => max_n_sub_records)
          else
            raise '"groupBy" parameter must be a Hash or a String'
          end
        end
        @count = @result.size
        if @query["sortBy"]
          if @query["sortBy"].is_a? Array
            keys = parse_order_keys(@query["sortBy"])
            offset = 0
            limit = -1
          elsif @query["sortBy"].is_a? Hash
            keys = parse_order_keys(@query["sortBy"]["keys"])
            offset = @query["sortBy"]["offset"]
            limit = @query["sortBy"]["limit"]
          else
            raise '"sortBy" parameter must be a Hash or an Array'
          end
          @result = @result.sort(keys, :offset => offset, :limit => limit)
        end
        @result
      end

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

      def format_records(formatted_result)
        return unless need_element_output?("records")

        params = @query["output"]

        attributes = params["attributes"]
        target_attributes = normalize_target_attributes(attributes)
        offset = params["offset"] || 0
        limit = params["limit"] || 10
        @result.open_cursor(:offset => offset, :limit => limit) do |cursor|
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
            record.collect do |sub_record|
              target_attributes = resolve_attributes(attribute, sub_record)
              complex_record(target_attributes, sub_record)
            end
          else
            record.collect do |sub_record|
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
            record[attribute[:source]]
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

      def accessor_name?(source)
        /\A[a-zA-Z\#@$_][a-zA-Z\d\#@$_\-.]*\z/ === source
      end

      def normalize_target_attributes(attributes, domain = @result)
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
    end
  end
end
