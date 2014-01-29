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
        output = output["output"]
      end
      emit(output, result)
    end

    command :collector_reduce
    def collector_reduce(request)
      body[input_name].each do |output, deal|
        left_value = output_values[output]
        right_value = request
        value = reduce(deal, left_value, right_value)
        emit(output, value)
      end
    end

    def reduce(deal, left_value, right_value)
      if left_value.nil? || right_value.nil?
        return right_value || left_value
      end

      reduced_value = nil

      case deal["type"]
      when "and"
        reduced_value = left_value && right_value
      when "or"
        reduced_value = left_value || right_value
      when "sum"
        reduced_value = sum(left_value, right_value)
        reduced_value = apply_output_range(reduced_value,
                                           "limit" => deal["limit"])
      when "average"
        reduced_value = (left_value.to_f + right_value.to_f) / 2
      when "sort"
        reduced_value = merge(left_value,
                              right_value,
                              :operators => deal["operators"],
                              :key_column => deal["key_column"])
        reduced_value = apply_output_range(reduced_value,
                                           "limit" => deal["limit"])
      end

      reduced_value
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

    def sum(x, y)
      return x || y if x.nil? or y.nil?

      if x.is_a?(Hash) && y.is_a?(Hash)
        x.merge(y)
      else
        x + y
      end
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
              next if column == key_column_index
              unified_item[column] += value
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
