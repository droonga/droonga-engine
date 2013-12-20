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
require "droonga/input_message"

module Droonga
  class Dispatcher
    attr_reader :name, :envelope, :collectors

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

    def add_route(route)
      envelope["via"].push(route)
    end

    def handle_envelope(envelope)
      @envelope = envelope
      if envelope["type"] == "dispatcher"
        handle(envelope["body"], envelope["arguments"])
      else
        process_input_message(envelope)
      end
    end

    def post(body, destination=nil)
      $log.trace("#{log_tag}: post: start")
      route = nil
      unless is_route?(destination)
        route = envelope["via"].pop
        destination = route
      end
      unless is_route?(destination)
        destination = envelope["replyTo"]
      end
      command = nil
      receiver = nil
      arguments = nil
      synchronous = nil
      case destination
      when String
        command = destination
      when Hash
        command = destination["type"]
        receiver = destination["to"]
        arguments = destination["arguments"]
        synchronous = destination["synchronous"]
      end
      if receiver
        @forwarder.forward(envelope.merge("body" => body),
                           "type" => command,
                           "to" => receiver,
                           "arguments" => arguments)
      else
        if command == "dispatcher"
          handle(body, arguments)
        elsif @output_adapter.processable?(command)
          @output_adapter.process(command, body, *arguments)
        end
      end
      add_route(route) if route
      $log.trace("#{log_tag}: post: done")
    end

    def handle(message, arguments)
      case message
      when Hash
        handle_internal_message(message)
      end
    end

    def handle_internal_message(message)
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
    end

    def dispatch(message, destination)
      if local?(destination)
        handle_internal_message(message)
      else
        @forwarder.forward(envelope.merge("body" => message),
                           "type" => "dispatcher",
                           "to"   => farm_path(destination))
      end
    end

    # TODO: Use more meaningful name
    def process_in_farm(route, message, type, synchronous)
      # TODO: validate route is farm path
      envelope = @envelope.merge("body" => message, "type" => type)
      @farm.process(route, envelope, synchronous)
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

    def apply_input_adapters(envelope)
      adapted_envelope = envelope
      loop do
        input_message = InputMessage.new(adapted_envelope)
        command = input_message.command
        break unless @input_adapter.processable?(command)
        @input_adapter.process(command, input_message)
        new_command = input_message.command
        adapted_envelope = input_message.adapted_envelope
        break if command == new_command
      end
      adapted_envelope
    end

    def process_input_message(envelope)
      adapted_envelope = apply_input_adapters(envelope)
      @distributor.process(adapted_envelope["type"], adapted_envelope)
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
