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

    UNLIMITED = -1.freeze

    command :collector_search_gather
    def collector_search_gather(result)
      output = body ? body[input_name] : input_name
      if output.is_a?(Hash)
        elements = output["elements"]
        if elements && elements.is_a?(Hash)
          # phase 1: pre-process
          elements.each do |element, mapper|
            case mapper["type"]
            when "count"
              result[element] = result[mapper["target"]].size
            when "sort"
              # do nothing on this phase!
            end
          end
          # phase 2: post-process
          elements.each do |element, mapper|
            if mapper["no_output"]
              result.delete(element)
              next
            end

            case mapper["type"]
            when "count"
              # do nothing on this phase!
            when "sort"
              # because "count" type mapper requires all items of the array,
              # I have to apply "sort" type mapper later.
              if result[element]
                result[element] = apply_output_range(result[element], mapper)
                result[element] = apply_output_attributes_and_format(result[element], mapper)
              end
            end
          end
        end
        output = output["output"]
      end
      emit(output, result)
    end

    def apply_output_range(items, output)
      if items && items.is_a?(Array)
        offset = output["offset"] || 0
        unless offset.zero?
          items = items[offset..-1] || []
        end

        limit = output["limit"] || 0
        unless limit == UNLIMITED
          items = items[0...limit]
        end
      end
      items
    end

    def apply_output_attributes_and_format(items, output)
      attributes = output["attributes"]
      if attributes
        format = output["format"]
        if format == "complex"
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
      end
      items
    end

    command :collector_search_reduce
    def collector_search_reduce(request)
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
        result[key] = reduce(deal, left_values[key], right_values[key])
      end
      result
    end
  end
end
