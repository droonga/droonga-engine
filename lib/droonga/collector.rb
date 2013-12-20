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

require "droonga/pluggable"
require "droonga/collector_plugin"

module Droonga
  class Collector
    include Pluggable

    def initialize(id, dispatcher, components, tasks, inputs)
      @id = id
      @dispatcher = dispatcher
      @components = components
      @tasks = tasks
      @n_dones = 0
      @inputs = inputs
      load_plugins(["basic"]) # TODO: make customizable
    end

    def done?
      @n_dones == @tasks.size
    end

    def start
      tasks = @inputs[nil]
      tasks.each do |task|
        component = task["component"]
        type = component["type"]
        command = component["command"]
        n_of_expects = component["n_of_expects"]
        synchronous = nil
        descendants = {}
        component["descendants"].each do |name, indices|
          descendants[name] = indices.collect do |index|
            @components[index]["routes"].map do |route|
              @dispatcher.farm_path(route)
            end
          end
        end
        message = {
          "id"          => @id,
          "task"        => task,
          "descendants" => descendants
        }
        @dispatcher.process_in_farm(task["route"], message, command, synchronous)
        @n_dones += 1
      end
    end

    def receive(name, value)
      tasks = @inputs[name]
      unless tasks
        #TODO: result arrived before its query
        return
      end
      tasks.each do |task|
        task["n_of_inputs"] += 1
        component = task["component"]
        type = component["type"]
        command = "collector_" + type
        n_of_expects = component["n_of_expects"]
        message = {
          "task"=>task,
          "name"=>name,
          "value"=>value
        }
        process(command, message)
        return if task["n_of_inputs"] < n_of_expects
        #the task is done
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
        @n_dones += 1
      end
    end

    private
    def instantiate_plugin(name)
      CollectorPlugin.repository.instantiate(name, @dispatcher)
    end

    def log_tag
      "collector"
    end
  end
end
