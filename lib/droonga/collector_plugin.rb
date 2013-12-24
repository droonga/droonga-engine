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
  class CollectorPlugin < Plugin
    extend PluginRegisterable

    attr_reader :task, :input_name, :component, :output_values, :body, :output_names
    def initialize
      super()
    end

    def process(command, message)
      return false unless message.is_a? Hash
      @task = message["task"]
      return false unless @task.is_a? Hash
      @component = @task["component"]
      return false unless @component.is_a? Hash
      @output_values = @task["values"]
      @body = @component["body"]
      @output_names = @component["outputs"]
      @id = message["id"]
      @value = message["value"]
      @input_name = message["name"]
      super(command, @value)
      true
    end

    # TODO: consider better name
    def emit(name, value)
      @output_values[name] = value
    end
  end
end
