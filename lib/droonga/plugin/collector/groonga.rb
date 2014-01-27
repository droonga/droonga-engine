# -*- coding: utf-8 -*-
#
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
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

require "droonga/plugin/collector/basic"

module Droonga
  class GroongaCollector < BasicCollector
    repository.register("groonga", self)

    command :collector_groonga_gather
    def collector_groonga_gather(result)
      collector_gather(result)
    end

    command :collector_groonga_reduce
    def collector_groonga_reduce(request)
      collector_reduce(request)
    end

    def reduce(deal, left_value, right_value)
      reduced_value = nil

      case deal["type"]
      when "groonga_result"
        #XXX how to merge multiple erros?
        #XXX how to mix regular results and erros?
        # reduced_value = merge_groonga_result(left_value, right_value)
        reduced_value = left_value || right_value
      else
        reduced_value = super
      end

      reduced_value
    end

    def merge_groonga_result(left_value, right_value)
      result = []

      result << merge_groonga_header(left_value.shift, right_value.shift)

      left_value.each_with_index do |left, index|
        right = right_value[index]
        result << reduce({ "type" => "and" }, left, right)
      end

      result
    end

    def merge_groonga_header(left_header, right_header)
      status = [left_header.shift, right_header.shift].min

      start_time = reduce({ "type" => "average" },
                          left_header.shift,
                          right_header.shift)

      elapsed_time = reduce({ "type" => "average" },
                            left_header.shift,
                            right_header.shift)

      #XXX we should merge error informations more smarter...
      error_information = reduce({ "type" => "sum",
                                   "limit" => UNLIMITED },
                                 left_header,
                                 right_header)

      [status, start_time, elapsed_time] + error_information
    end
  end
end
