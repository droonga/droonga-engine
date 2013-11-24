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
    def initialize(dispatcher)
      super()
      @dispatcher = dispatcher
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
      @descendants = message["descendants"]
      super(command, @value)
      output if @descendants
      true
    end

    # TODO: consider better name
    def emit(value, name=nil)
      unless name
        if @output_names
          name = @output_names.first
        else
          @output_values = @task["values"] = value
          return
        end
      end
      @output_values[name] = value
    end

    def post(message, destination=nil)
      @distributor.post(message, destination)
    end

    def output
      result = @task["values"]
      post(result, @component["post"]) if @component["post"]
      @descendants.each do |name, dests|
        message = {
          "id" => @id,
          "input" => name,
          "value" => result[name]
        }
        dests.each do |routes|
          routes.each do |route|
            post(message, "to"=>route, "type"=>"dispatcher")
          end
        end
      end
    end
  end
end
