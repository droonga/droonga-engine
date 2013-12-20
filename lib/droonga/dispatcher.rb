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

require 'tsort'
require "droonga/input_adapter"
require "droonga/output_adapter"
require "droonga/distributor"
require "droonga/catalog"
require "droonga/collector"
require "droonga/farm"

module Droonga
  class Dispatcher
    attr_reader :name

    def initialize(options)
      @options = options
      @name = @options[:name]
      @farm = Farm.new(name)
      @collectors = {}
      @current_id = 0
      @local = Regexp.new("^#{@name}")
      @input_adapter =
        InputAdapter.new(self, :plugins => Droonga.catalog.option("plugins"))
      @output_adapter =
        OutputAdapter.new(self, :plugins => Droonga.catalog.option("plugins"))
      @loop = EventLoop.new
      @forwarder = Forwarder.new(@loop)
      @distributor = Distributor.new(self, @options)
    end

    def start
      @forwarder.start
      @farm.start
      @loop_thread = Thread.new do
        @loop.run
      end
    end

    def shutdown
      @forwarder.shutdown
      @distributor.shutdown
      @input_adapter.shutdown
      @output_adapter.shutdown
      @farm.shutdown
      @loop.stop
      @loop_thread.join
    end

    def handle_message(message)
      @message = message
      if message["type"] == "dispatcher"
        process_internal_message(message["body"])
      else
        process_input_message(message)
      end
    end

    def forward(message, destination)
      $log.trace("#{log_tag}: forward start")
      @forwarder.forward(message, destination)
      $log.trace("#{log_tag}: forward done")
    end

    def reply(body)
      adapted_message = @output_adapter.adapt(@message.merge("body" => body))
      @forwarder.forward(adapted_message,
                         adapted_message["replyTo"])
    end

    def process_internal_message(message)
      id = message["id"]
      collector = @collectors[id]
      if collector
        collector.receive(message["input"], message["value"])
      else
        components = message["components"]
        if components
          planner = Planner.new(self, components)
          collector = planner.create_collector(id)
          @collectors[id] = collector
        else
          #todo: take cases receiving result before its query into account
        end
        collector.start
      end
      @collectors.delete(id) if collector.done?
    end

    def dispatch(message, destination)
      if local?(destination)
        process_internal_message(message)
      else
        @forwarder.forward(@message.merge("body" => message),
                           "type" => "dispatcher",
                           "to"   => farm_path(destination))
      end
    end

    # TODO: Use more meaningful name
    def process_in_farm(route, message, type, synchronous)
      # TODO: validate route is farm path
      message = @message.merge("body" => message, "type" => type)
      @farm.process(route, message, synchronous)
    end

    def generate_id
      id = @current_id
      @current_id = id.succ
      return [@name, id].join('.#')
    end

    def farm_path(route)
      if route =~ /\A.*:\d+\/[^\.]+/
        $&
      else
        route
      end
    end

    def local?(route)
      route =~ @local
    end

    private
    def is_route?(route)
      route.is_a?(String) || route.is_a?(Hash)
    end

    def process_input_message(message)
      adapted_message = @input_adapter.adapt(message)
      @distributor.process(adapted_message["type"], adapted_message)
    end

    def log_tag
      "[#{Process.ppid}][#{Process.pid}] dispatcher"
    end

    class Planner
      attr_reader :components

      def initialize(dispatcher, components)
        @dispatcher = dispatcher
        @components = components
      end

      def create_collector(id)
        resolve_descendants
        tasks = []
        inputs = {}
        @components.each do |component|
          component["routes"].each do |route|
            next unless @dispatcher.local?(route)
            task = {
              "route" => route,
              "component" => component,
              "n_of_inputs" => 0,
              "values" => {}
            }
            tasks << task
            (component["inputs"] || [nil]).each do |input|
              inputs[input] ||= []
              inputs[input] << task
            end
          end
        end
        collector = Collector.new(id, @dispatcher, @components, tasks, inputs)
        return collector
      end

      def resolve_descendants
        @descendants = {}
        @components.size.times do |index|
          component = @components[index]
          (component["inputs"] || []).each do |input|
            @descendants[input] ||= []
            @descendants[input] << index
          end
          component["n_of_expects"] = 0
        end
        @components.each do |component|
          descendants = get_descendants(component)
          component["descendants"] = descendants
          descendants.each do |key, indices|
            indices.each do |index|
              @components[index]["n_of_expects"] += component["routes"].size
            end
          end
        end
      end

      def get_descendants(component)
        return {} unless component["outputs"]
        descendants = {}
        component["outputs"].each do |output|
          descendants[output] = @descendants[output]
        end
        descendants
      end
    end
  end
end
