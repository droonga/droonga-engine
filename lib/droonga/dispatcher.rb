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
require "droonga/legacy_plugin"
require "droonga/adapter"
require "droonga/catalog"
require "droonga/collector"
require "droonga/farm"

module Droonga
  class Dispatcher
    attr_reader :collectors
    def initialize(worker, name)
      @farm = Farm.new(name)
      @farm.start
      @worker = worker
      @name = name
      @collectors = {}
      @current_id = 0
      @local = Regexp.new("^#{@name}")
      plugins = ["collector"] + (Droonga.catalog.option("plugins")||[]) + ["adapter"]
      plugins.each do |plugin|
        @worker.add_legacy_plugin(plugin)
      end
    end

    def shutdown
      @farm.shutdown
    end

    def handle(message, arguments)
      case message
      when Array
        handle_incoming_message(message)
      when Hash
        handle_internal_message(message)
      end
    end

    def handle_incoming_message(message)
      id = generate_id
      planner = Planner.new(self, message)
      destinations = planner.resolve(id)
      components = planner.components
      message = { "id" => id, "components" => components }
      destinations.each do |destination, frequency|
        dispatch(message, destination)
      end
    end

    def handle_internal_message(message)
      id = message["id"]
      collector = @collectors[id]
      unless collector
        components = message["components"]
        if components
          planner = Planner.new(self, components)
          collector = planner.get_collector(id)
        else
          #todo: take cases receiving result before its query into account
        end
      end
      collector.handle(message["input"], message["value"])
    end

    def dispatch(message, destination)
      if local?(destination)
        handle_internal_message(message)
      else
        post(message, "to"=>farm_path(destination), "type"=>"dispatcher")
      end
    end

    def deliver(id, route, message, type, synchronous)
      if id == route
        post(message, "type" => type, "synchronous"=> synchronous)
      else
        envelope = @worker.envelope.merge("body" => message, "type" => type)
        @farm.process(route, envelope, synchronous)
      end
    end

    def post(message, destination)
      @worker.post(message, destination)
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

    class Planner
      attr_reader :components
      class UndefinedInputError < StandardError
        attr_reader :input
        def initialize(input)
          @input = input
          super("undefined input assigned: <#{input}>")
        end
      end

      class CyclicComponentsError < StandardError
        attr_reader :components
        def initialize(components)
          @components = components
          super("cyclic components found: <#{components}>")
        end
      end

      include TSort
      def initialize(dispatcher, components)
        @dispatcher = dispatcher
        @components = components
      end

      def resolve(id)
        @dependency = {}
        @components.each do |component|
          @dependency[component] = component["inputs"]
          next unless component["outputs"]
          component["outputs"].each do |output|
            @dependency[output] = [component]
          end
        end
        @components = []
        each_strongly_connected_component do |cs|
          raise CyclicComponentsError.new(cs) if cs.size > 1
          @components.concat(cs) unless cs.first.is_a? String
        end
        resolve_routes(id)
      end

      def resolve_routes(id)
        local = [id]
        destinations = Hash.new(0)
        @components.each do |component|
          dataset = component["dataset"]
          routes =
            if dataset
              Droonga.catalog.get_routes(dataset, component)
            else
              local
            end
          routes.each do |route|
            destinations[@dispatcher.farm_path(route)] += 1
          end
          component["routes"] = routes
        end
        return destinations
      end

      def get_collector(id)
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
        @dispatcher.collectors[id] = collector
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

      def tsort_each_node(&block)
        @dependency.each_key(&block)
      end

      def tsort_each_child(node, &block)
        if node.is_a? String and @dependency[node].nil?
          raise UndefinedInputError.new(node)
        end
        if @dependency[node]
          @dependency[node].each(&block)
        end
      end
    end
  end

  class DispatcherMessageHandler < Droonga::LegacyPlugin
    Droonga::LegacyPlugin.repository.register("dispatcher_message", self)
    def initialize(*arguments)
      super
      @dispatcher = Droonga::Dispatcher.new(@worker, @worker.name)
    end

    def shutdown
      @dispatcher.shutdown
    end

    command :dispatcher
    def dispatcher(request, *arguments)
      @dispatcher.handle(request, arguments)
    end

    def prefer_synchronous?(command)
      return true
    end
  end
end
