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

require "droonga/legacy_plugin"
require "droonga/searcher"

module Droonga
  class BasicCollectorHandler < Droonga::CollectorHandler
    Droonga::HandlerPlugin.repository.register("collector", self)

    command :collector_gather
    def collector_gather(request)
      output = body ? body[input_name] : input_name
      emit(request, output)
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
        func, *args = deal
        case func
        when "sum"
          result[key] = values[0][key] + values[1][key]
        when "sort"
          result[key] = merge(values[0][key], values[1][key], args)
        end
      end
      return result
    end

    def merge(x, y, order)
      index = 0
      y.each do |_y|
        loop do
          _x = x[index]
          break unless _x
          break if compare(_y, _x, order)
          index += 1
        end
        x.insert(index, _y)
        index += 1
      end
      return x
    end

    def compare(x, y, operators)
      for index in 0..x.size-1 do
        _x = x[index]
        _y = y[index]
        operator = operators[index]
        break unless operator
        return true if _x.__send__(operator, _y)
      end
      return false
    end
  end
end
