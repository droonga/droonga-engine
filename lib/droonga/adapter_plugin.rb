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

require "droonga/plugin"

module Droonga
  class AdapterPlugin < Plugin
    extend PluginRegisterable

    def initialize(executor)
      super()
      @executor = executor
    end

    def add_route(route)
      @executor.add_route(route)
    end

    def post(body, destination=nil)
      @executor.post(body, destination)
    end

    def emit(value, name=nil)
      if name
        @output_values[name] = value
      else
        @output_values = value
      end
    end

    def process(command, message)
      @output_values = {}
      super(command, message)
      post(@output_values) unless @output_values.empty?
    end
  end
end
