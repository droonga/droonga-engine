# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 droonga project
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

module Droonga
  class Proxy
    attr_reader :collectors
    def initialize(worker, name)
      @worker = worker
      @name = name
      @collectors = {}
      @current_id = 0
      @local = Regexp.new("^#{@name}")
    end

    def handle(message)
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
        dispatch(destination, message)
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

    def dispatch(destination, message)
      if local?(destination)
        handle_internal_message(message)
      else
        post(farm_path(destination), message)
      end
    end

    def post(route, message)
      @worker.post(message, "to"=> route, "type"=>"proxy")
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
      def initialize(proxy, components)
        @proxy = proxy
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
              Droonga::catalog.get_routes(dataset, component)
            else
              local
            end
          routes.each do |route|
            destinations[@proxy.farm_path(route)] += 1
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
            next unless @proxy.local?(route)
            task = {
              "route" => route,
              "component" => component,
              "n_of_inputs" => 0,
              "values" => []
            }
            tasks << task
            (component["inputs"] || [nil]).each do |input|
              inputs[input] ||= []
              inputs[input] << task
            end
          end
        end
        collector = Collector.new(id, @proxy, @components, tasks, inputs)
        @proxy.collectors[id] = collector
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

    class Collector
      def initialize(id, proxy, components, tasks, inputs)
        @id = id
        @proxy = proxy
        @components = components
        @tasks = tasks
        @n_dones = 0
        @inputs = inputs
      end

      def handle(name, value)
        tasks = @inputs[name]
        tasks.each do |task|
          if name
            task["values"] << value
            task["n_of_inputs"] += 1
          end
          component = task["component"]
          return if task["n_of_inputs"] < component["n_of_expects"]
          result = task["values"]
          component["descendants"].each do |name, indices|
            message = {
              "id" => @id,
              "input" => name,
              "value" => result
            }
            indices.each do |index|
              dest = @components[index]
              routes = dest["routes"]
              routes.each do |route|
                @proxy.dispatch(route, message)
              end
            end
          end
          @n_dones += 1
          @proxy.collectors.delete(@id) if @n_dones == @tasks.size
        end
      end
    end
  end
end
