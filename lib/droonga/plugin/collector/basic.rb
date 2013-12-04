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

require "droonga/collector_plugin"

module Droonga
  class BasicCollector < Droonga::CollectorPlugin
    repository.register("basic", self)

    UNLIMITED = -1

    command :collector_gather
    def collector_gather(result)
      output = body ? body[input_name] : input_name
      if output.is_a?(Hash)
        element = output["element"]
        if element
          result[element] = apply_output_range(result[element], output)
          result[element] = apply_output_attributes_and_format(result[element], output)
        end
        output = output["output"]
      end
      emit(result, output)
    end

    def apply_output_range(items, output)
      if items && items.is_a?(Array)
        offset = output["offset"] || 0
        unless offset.zero?
          items = items[offset..-1]
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

    command :collector_reduce
    def collector_reduce(request)
      return unless request
      body[input_name].each do |output, elements|
        value = request
        old_value = output_values[output]
        value = reduce(elements, old_value, request) if old_value
        emit(value, output)
      end
    end

    def reduce(elements, *values)
      result = {}
      elements.each do |key, deal|
        reduced_values = nil

        case deal["type"]
        when "sum"
          reduced_values = values[0][key] + values[1][key]
        when "sort"
          reduced_values = merge(values[0][key],
                                 values[1][key],
                                 :operators => deal["operators"],
                                 :key_column => deal["key_column"],
                                 :unified_columns => deal["unified_columns"])
        end

        reduced_values = apply_output_range(reduced_values, "limit" => deal["limit"])

        result[key] = reduced_values
      end
      return result
    end

    def merge(x, y, options={})
      operators = options[:operators] = normalize_operators(options[:operators])

      unify_by_key!(x, y, options)

      index = 0
      y.each do |_y|
        loop do
          _x = x[index]
          break unless _x
          break if compare(_y, _x, operators)
          index += 1
        end
        x.insert(index, _y)
        index += 1
      end
      return x
    end

    def normalize_operators(operators)
      operators ||= []
      operators.collect do |operator|
        if operator.is_a?(String)
          { "operator" => operator }
        else
          operator
        end
      end
    end

    def compare(x, y, operators)
      operators.each_with_index do |operator, index|
        column = operator["column"] || index
        operator = operator["operator"]
        _x = x[column]
        _y = y[column]
        return true if _x.__send__(operator, _y)
      end
      return false
    end

    def unify_by_key!(base_items, unified_items, options={})
      key_column_index = options[:key_column]
      return unless key_column_index

      # The unified records must be smaller than the base, because
      # I sort unified records at last. I want to sort only smaller array.
      if base_items.size < unified_items.size
        base_items, unified_items = unified_items, base_items
      end

      rest_unified_items = unified_items.dup

      base_items.reject! do |base_item|
        key = base_item[key_column_index]
        rest_unified_items.any? do |unified_item|
          if unified_item[key_column_index] == key
            base_item.each_with_index do |value, column|
              if options[:unified_columns].include?(column)
                unified_item[column] += value
              else
                unified_item[column] ||= value
              end
            end
            rest_unified_items -= [unified_item]
            true
          else
            false
          end
        end
      end

      unless rest_unified_items.size == unified_items.size
        unified_items.sort! do |a, b|
          if compare(a, b, options[:operators])
            -1
          else
            1
          end
        end
      end
    end
  end
end
