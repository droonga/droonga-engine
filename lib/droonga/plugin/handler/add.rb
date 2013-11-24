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

require "groonga"

require "droonga/legacy_plugin"

module Droonga
  class AddHandler < Droonga::LegacyPlugin
    Droonga::LegacyPlugin.repository.register("add", self)

    command :add
    def add(request)
      outputs = process(request)
      emit(outputs)
    end

    private
    def process(request)
      table = @context[request["table"]]
      return [false] unless table
      if table.support_key?
        table.add(request["key"], request["values"])
      else
        table.add(request["values"])
      end
      [true]
    end
  end
end
