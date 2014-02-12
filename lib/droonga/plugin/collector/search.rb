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

require "droonga/plugin/collector/basic"

module Droonga
  class SearchCollector < BasicCollector
    repository.register("search", self)

    command :search_gather
    def search_gather(result)
      output = body ? body[input_name] : input_name
      if output.is_a?(Hash)
        elements = output["elements"]
        if elements && elements.is_a?(Hash)
          # because "count" mapper requires all records,
          # I have to apply it at first, before "limit" and "offset" are applied.
          count_mapper = elements["count"]
          if count_mapper
            if count_mapper["no_output"]
              result.delete("count")
            else
              result["count"] = result[count_mapper["target"]].size
            end
          end

          records_mapper = elements["records"]
          if records_mapper && result["records"]
            if records_mapper["no_output"]
              result.delete("records")
            else
              result["records"] = apply_output_range(result["records"], records_mapper)
              result["records"] = apply_output_attributes_and_format(result["records"], records_mapper)
            end
          end
        end
        output = output["output"]
      end
      emit(output, result)
    end

    def apply_output_attributes_and_format(items, output)
      attributes = output["attributes"] || []
      if output["format"] == "complex"
        items.collect! do |item|
          complex_item = {}
          attributes.each_with_index do |label, index|
            complex_item[label] = item[index]
          end
          complex_item
        end
      else
        items.collect! do |item|
          item[0...attributes.size]
        end
      end
      items
    end

    command :search_reduce
    def search_reduce(request)
      #XXX This is just a workaround. Errors should be handled by the framework itself.
      if input_name == "errors"
        return collector_reduce(request)
      end

      return unless request
      body[input_name].each do |output, elements|
        value = request
        old_value = output_values[output]
        value = reduce_elements(elements, old_value, request) if old_value
        emit(output, value)
      end
    end

    def reduce_elements(elements, left_values, right_values)
      result = {}
      elements.each do |key, deal|
        result[key] = reduce_value(deal, left_values[key], right_values[key])
      end
      result
    end
  end
end
