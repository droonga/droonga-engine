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

require "droonga/handler"

module Droonga
  class Collector
    def initialize(id, dispatcher, components, tasks, inputs)
      @id = id
      @dispatcher = dispatcher
      @components = components
      @tasks = tasks
      @n_dones = 0
      @inputs = inputs
    end

    def handle(name, value)
      tasks = @inputs[name]
      unless tasks
        #TODO: result arrived before its query
        return
      end
      tasks.each do |task|
        task["n_of_inputs"] += 1 if name
        component = task["component"]
        type = component["type"]
        command = component["command"] || ("collector_" + type)
        n_of_expects = component["n_of_expects"]
        synchronous = nil
        if command
          # TODO: should be controllable for each command respectively.
          synchronous = !n_of_expects.zero?
          # TODO: check if asynchronous execution is available.
          message = {
            "task"=>task,
            "name"=>name,
            "value"=>value
          }
          unless synchronous
            descendants = {}
            component["descendants"].each do |name, indices|
              descendants[name] = indices.collect do |index|
                @components[index]["routes"].map do |route|
                  @dispatcher.farm_path(route)
                end
              end
            end
            message["descendants"] = descendants
            message["id"] = @id
          end
          @dispatcher.deliver(@id, task["route"], message, command, synchronous)
        end
        return if task["n_of_inputs"] < n_of_expects
        #the task is done
        if synchronous
          result = task["values"]
          post = component["post"]
          @dispatcher.post(result, post) if post
          component["descendants"].each do |name, indices|
            message = {
              "id" => @id,
              "input" => name,
              "value" => result[name]
            }
            indices.each do |index|
              @components[index]["routes"].each do |route|
                @dispatcher.dispatch(message, route)
              end
            end
          end
        end
        @n_dones += 1
        @dispatcher.collectors.delete(@id) if @n_dones == @tasks.size
      end
    end
  end

  class CollectorHandler < Droonga::Handler
    attr_reader :task, :input_name, :component, :output_values, :body, :output_names
    def handle(command, request, *arguments)
      return false unless request.is_a? Hash
      @task = request["task"]
      return false unless @task.is_a? Hash
      @component = @task["component"]
      return false unless @component.is_a? Hash
      @output_values = @task["values"]
      @body = @component["body"]
      @output_names = @component["outputs"]
      @id = request["id"]
      @value = request["value"]
      @input_name = request["name"]
      @descendants = request["descendants"]
      invoke(command, @value, *arguments)
      output if @descendants
      true
    end

    def prefer_synchronous?(command)
      return true
    end
  end
end
