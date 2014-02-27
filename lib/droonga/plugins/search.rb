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

require "droonga/plugin"
require "droonga/searcher"
require "droonga/plugins/search/distributed_search_planner"

module Droonga
  module Plugins
    module Search
      extend Plugin
      register("search")

      class Planner < Droonga::Planner
        def plan(message)
          planner = DistributedSearchPlanner.new(message)
          planner.plan
        end
      end

      class Handler < Droonga::Handler
        def handle(message)
          searcher = Droonga::Searcher.new(@context)
          values = {}
          request = message.request
          raise Droonga::Searcher::NoQuery.new unless request
          searcher.search(request["queries"]).each do |output, value|
            values[output] = value
          end
          values
        end
      end

      class GatherCollector < Droonga::Collector
        message.pattern = ["task.step.type", :equal, "search_gather"]

        def collect(message)
          output = message.input || message.name
          if output.is_a?(Hash)
            elements = output["elements"]
            if elements and elements.is_a?(Hash)
              # because "count" mapper requires all records,
              # I have to apply it at first, before "limit" and "offset" are applied.
              body = message.body
              value = message.value
              count_mapper = elements["count"]
              if count_mapper
                if count_mapper["no_output"]
                  value.delete("count")
                else
                  value["count"] = value[count_mapper["target"]].size
                end
              end

              records_mapper = elements["records"]
              if records_mapper and value["records"]
                if records_mapper["no_output"]
                  value.delete("records")
                else
                  value["records"] = Reducer.apply_range(value["records"],
                                                         records_mapper)
                  value["records"] = apply_output_attributes_and_format(value["records"], records_mapper)
                end
              end
            end
            output_name = output["output"]
          else
            output_name = output
          end
          message.values[output_name] = message.value
        end

        private
        def apply_output_attributes_and_format(items, output)
          attributes = output["attributes"] || []
          if output["format"] == "complex"
            items.collect do |item|
              complex_item = {}
              attributes.each_with_index do |label, index|
                complex_item[label] = item[index]
              end
              complex_item
            end
          else
            items.collect do |item|
              item[0...attributes.size]
            end
          end
        end
      end

      class ReduceCollector < Droonga::Collector
        message.pattern = ["task.step.type", :equal, "search_reduce"]

        def collect(message)
          #XXX This is just a workaround. Errors should be handled by the framework itself.
          if message.name == "errors"
            basic_reduce_collector = Basic::ReduceCollector.new
            return basic_reduce_collector.collect(message)
          end

          message.input.each do |output_name, elements|
            old_value = message.values[output_name]
            if old_value
              value = reduce_elements(elements, old_value, message.value)
            else
              value = message.value
            end
            message.values[output_name] = value
          end
        end

        def reduce_elements(elements, left_values, right_values)
          result = {}
          elements.each do |key, deal|
            reducer = Reducer.new(deal)
            result[key] = reducer.reduce(left_values[key], right_values[key])
          end
          result
        end
      end

      define_single_step do |step|
        step.name = "search"
        step.handler = Handler
      end
    end
  end
end
