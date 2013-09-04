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
require "droonga/handler"

module Droonga
  class Proxy
    attr_reader :collectors
    def initialize(worker, name)
      @engines = {}
      Droonga::catalog.get_engines(name).each do |name, options|
        engine = Droonga::Engine.new(options.merge(:proxy => false,
                                                   :with_server => false))
        engine.start
        @engines[name] = engine
      end
      @worker = worker
      @name = name
      @collectors = {}
      @current_id = 0
      @local = Regexp.new("^#{@name}")
    end

    def shutdown
      @engines.each do |name, engine|
        engine.shutdown
      end
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
        post(message, "to"=>farm_path(destination), "type"=>"proxy")
      end
    end

    def deliver(id, route, message, type, synchronous)
      if id == route
        post(message, "type" => type, "synchronous"=> synchronous)
      else
        envelope = @worker.envelope.merge("body" => message, "type" => type)
        @engines[route].emit('', Time.now.to_f, envelope, synchronous)
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
              "values" => {}
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
          task["n_of_inputs"] += 1 if name
          component = task["component"]
          type = component["type"]
          command = component["command"] || ("proxy_" + type)
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
                    @proxy.farm_path(route)
                  end
                end
              end
              message["descendants"] = descendants
              message["id"] = @id
            end
            @proxy.deliver(@id, task["route"], message, command, synchronous)
          end
          return if task["n_of_inputs"] < n_of_expects
          #the task is done
          if synchronous
            result = task["values"]
            post = component["post"]
            @proxy.post(result, post) if post
            component["descendants"].each do |name, indices|
              message = {
                "id" => @id,
                "input" => name,
                "value" => result[name]
              }
              indices.each do |index|
                @components[index]["routes"].each do |route|
                  @proxy.dispatch(message, route)
                end
              end
            end
          end
          @n_dones += 1
          @proxy.collectors.delete(@id) if @n_dones == @tasks.size
        end
      end
    end
  end

  class ProxyMessageHandler < Droonga::Handler
    Droonga::HandlerPlugin.register("proxy_message", self)
    def initialize(*arguments)
      super
      @proxy = Droonga::Proxy.new(@worker, @worker.name)
    end

    def shutdown
      @proxy.shutdown
    end

    command :proxy
    def proxy(request, *arguments)
      @proxy.handle(request, arguments)
    end

    def prefer_synchronous?(command)
      return true
    end
  end

  class ProxyHandler < Droonga::Handler
    attr_reader :task, :input_name, :component, :output_values, :body, :output_names
    def handle(command, request, *arguments)
      @task = request["task"]
      @input_name = request["name"]
      @component = @task["component"]
      @output_names = @component["outputs"]
      @body = @component["body"]
      @output_values = @task["values"]
      @descendants = request["descendants"]
      @id = request["id"]
      super(command, request["value"], *arguments)
      output if @descendants
    end

    def emit(value, name = nil)
      unless name
        if @output_names
          name = @output_names.first
        else
          @task["values"] = value
          return
        end
      end
      @task["values"][name] = value
    end

    def output
      result = @task["values"]
      post = component["post"]
      post(result, post) if post
      @descendants.each do |name, dests|
        message = {
          "id" => @id,
          "input" => name,
          "value" => result[name]
        }
        dests.each do |routes|
          routes.each do |route|
            post(message, "to"=>route, "type"=>"proxy")
          end
        end
      end
    end

    def prefer_synchronous?(command)
      return true
    end
  end
end
