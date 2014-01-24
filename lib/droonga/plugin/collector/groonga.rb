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
      collector_reduce(result)
    end

    def reduce(deal, left_value, right_value)
      reduced_value = nil

      case deal["type"]
      when "groonga_result"
        reduced_value = left_value && right_value
      end

      reduced_value
    end
  end
end
