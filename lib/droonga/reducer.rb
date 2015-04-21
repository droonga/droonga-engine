# Copyright (C) 2014 Droonga Project
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

module Droonga
  class Reducer
    class << self
      # TODO: This is right location?
      def apply_range(items, range)
        if items and items.is_a?(Array)
          offset = range["offset"] || 0
          unless offset.zero?
            items = items[offset..-1] || []
          end

          limit = range["limit"] || 0
          unless limit == UNLIMITED
            items = items[0...limit]
          end
        end
        items
      end
    end

    # XXX: We has ULIMITED defined
    # lib/droonga/plugins/search/distributed_search_planner.rb. We
    # should unify it.
    UNLIMITED = -1

    def initialize(deal)
      @deal = deal # TODO: deal is good name?
    end

    def reduce(left_value, right_value)
      if left_value.nil? or right_value.nil?
        return right_value || left_value
      end

      reduced_value = nil

      case @deal["type"]
      when "and"
        reduced_value = (left_value and right_value)
      when "or"
        reduced_value = (left_value or right_value)
      when "sum"
        reduced_value = sum(left_value, right_value)
        reduced_value = self.class.apply_range(reduced_value,
                                               "limit" => @deal["limit"])
      when "recursive-sum"
        reduced_value = recursive_sum(left_value, right_value)
        reduced_value = self.class.apply_range(reduced_value,
                                               "limit" => @deal["limit"])
      when "average"
        reduced_value = (left_value.to_f + right_value.to_f) / 2
      when "sort"
        reduced_value = merge(left_value,
                              right_value,
                              :operators => @deal["operators"],
                              :key_column => @deal["key_column"])
        reduced_value = self.class.apply_range(reduced_value,
                                               "limit" => @deal["limit"])
      end

      reduced_value
    end

    private
    def sum(x, y)
      return x || y if x.nil? or y.nil?

      if x.is_a?(Hash) and y.is_a?(Hash)
        x.merge(y)
      else
        x + y
      end
    end

    def recursive_sum(x, y)
      return x || y if x.nil? or y.nil?

      if x.is_a?(Hash) and y.is_a?(Hash)
        if numeric_hash?(x) and numeric_hash?(y)
          sum_numeric_hashes(x, y)
        else
          x.merge(y)
        end
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

      unified_key_map = {}
      unified_items.each do |unified_item|
        unified_key_map[unified_item[key_column_index]] = unified_item
      end

      unified = false
      base_items.reject! do |base_item|
        key = base_item[key_column_index]
        unified_item = unified_key_map[key]
        if unified_item
          base_item.each_with_index do |value, column_index|
            next if column_index == key_column_index
            unified_item[column_index] += value
          end
          unified = true
          true
        else
          false
        end
      end

      if unified
        unified_items.sort! do |a, b|
          if compare(a, b, options[:operators])
            -1
          else
            1
          end
        end
      end
    end

    def numeric_hash?(hash)
      return false unless hash.is_a?(Hash)
      hash.values.all? do |value|
        case value
        when Numeric
          true
        when Hash
          numeric_hash?(value)
        else
          false
        end
      end
    end

    def sum_numeric_hashes(x, y)
      sum = {}
      (x.keys + y.keys).each do |key|
        x_value = x[key]
        y_value = y[key]
        if numeric_hash?(x_value) and numeric_hash?(y_value)
          sum[key] = sum_numeric_hashes(x_value, y_value)
        else
          sum[key] = (x_value || 0) + (y_value || 0)
        end
      end
      sum
    end
  end
end
