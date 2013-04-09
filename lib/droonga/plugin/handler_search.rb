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
      outputs = {}
      return outputs if queries.nil?

      query_sorter = QuerySorter.new
      queries.each do |name, query|
        query_sorter.add(name, [query["source"]])
      end
      results = {}
      query_sorter.tsort.each do |name|
        if queries[name]
          searcher = QuerySearcher.new(@context, queries[name])
          results[name] = searcher.search(results)
          outputs[name] = searcher.output if searcher.need_output?
        elsif @context[name]
          results[name] = @context[name]
        else
          raise UndefinedSourceError.new(name)
        end
      end
      outputs
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
      end

      def search(results)
        search_query(@query, results)
      end

      def parseCondition(source, expression, condition)
        if condition.is_a? String
          expression.parse(condition, :syntax => :script)
        elsif condition.is_a? Hash
          options = {}
          if condition["matchTo"]
            matchTo = Groonga::Expression.new(context: @context)
            matchTo.define_variable(:domain => source)
            matchTo.parse(condition["matchTo"], :syntax => :script)
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

      def parseOrderKeys(keys)
        keys.map do |key|
          if key =~ /^-/
            [$', :descending]
          else
            [key, :ascending]
          end
        end
      end

      def search_query(query, results)
        @start_time = Time.now
        result = source = results[query["source"]]
        if query["condition"]
          expression = Groonga::Expression.new(context: @context)
          expression.define_variable(:domain => source)
          parseCondition(source, expression, query["condition"])
          result = source.select(expression)
        end
        if query["groupBy"]
          result = result.group(query["groupBy"])
        end
        if query["sortBy"]
          if query["sortBy"].is_a? Array
            keys = parseOrderKeys(query["sortBy"])
            offset = 0
            limit = -1
          elsif query["sortBy"].is_a? Hash
            keys = parseOrderKeys(query["sortBy"]["keys"])
            offset = query["sortBy"]["offset"]
            limit = query["sortBy"]["limit"]
          else
            raise '"sortBy" parameter must be a Hash or an Array'
          end
          result = result.sort(keys, :offset => offset, :limit => limit)
        end
        @result = result
        result
      end

      def need_output?
        @query.has_key?("output")
      end

      def output
        return nil unless need_output?

        params = @query["output"]
        result = @result
        output = {}
        offset = params["offset"] || 0
        limit = params["limit"] || 10
        if params["count"]
          count = result.size
          output["count"] = count
        end
        if params["attributes"].is_a? Array
          attributes = params["attributes"].map do |attribute|
            if attribute.is_a?(String)
              { label: attribute, source: attribute}
            else
              { label: attribute["label"] || attribute["source"],
                source: attribute["source"] }
            end
          end
          output["records"] = result.open_cursor(:offset => offset,
                                                 :limit => limit) do |cursor|
            cursor.collect do |record|
              values = {}
              attributes.collect do |attribute|
                values[attribute[:label]] = record[attribute[:source]]
              end
              values
            end
          end
        end
        if params["elapsedTime"]
          output["startTime"] = @start_time.iso8601
          output["elapsedTime"] = Time.now.to_f - @start_time.to_f
        end
        output
      end
    end
  end
end
