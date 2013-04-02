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

    command :search
    def search(request)
      queries = request["queries"]
      results = {}
      outputs = {}
      query_sorter = QuerySorter.new
      queries.each do |name, query|
        query_sorter.add(name, [query["source"]])
      end
      query_sorter.tsort.each do |name|
        if queries[name]
          search_query(name, queries, results, outputs)
        elsif @context[name]
          results[name] = @context[name]
        else
          raise "undefined source(#{name}) was assigned"
        end
      end
      outputs
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

    def search_query(name, queries, results, outputs)
      start_time = Time.now
      query = queries[name]
      source = results[query["source"]]
      if query["condition"]
        expression = Groonga::Expression.new(context: @context)
        expression.define_variable(:domain => source)
        parseCondition(source, expression, query["condition"])
        results[name] = source.select(expression)
      else
        results[name] = source
      end
      if query["output"]
        result = results[name]
        offset = query["offset"] || 0
        limit = query["limit"] || 10
        outputs[name] = output = {}
        if query["output"]["count"]
          output["count"] = result.size
        end
        if query["output"]["result"]
          attributes = query["output"]["result"]["attributes"]
          if attributes
            attrs = attributes.map do |attr|
              if attr.is_a?(String)
                { label: attr, source: attr}
              else
                { label: attr["label"] || attr["source"],
                  source: attr["source"] }
              end
            end
            output["result"] = result.open_cursor(:offset => offset,
                                                  :limit => limit) do |cursor|
              cursor.collect do |record|
                values = {}
                attrs.collect do |attr|
                  values[attr[:label]] = record[attr[:source]]
                end
                values
              end
            end
          end
        end
        if query["output"]["elapsedTime"]
          output["startTime"] = start_time.iso8601
          output["elapsedTime"] = Time.now.to_f - start_time.to_f
        end
      end
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
  end
end
