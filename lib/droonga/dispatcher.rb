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

require "English"
require "tsort"

require "droonga/input_adapter"
require "droonga/output_adapter"
require "droonga/distributor"
require "droonga/catalog"
require "droonga/collector"
require "droonga/farm"
require "droonga/session"
require "droonga/replier"
require "droonga/message_processing_error"

module Droonga
  class Dispatcher
    attr_reader :name

    class MissingDatasetParameter < BadRequest
      def initialize
        super("\"dataset\" must be specified.")
      end
    end

    class UnknownDataset < NotFound
      def initialize(dataset)
        super("The dataset #{dataset.inspect} does not exist.")
      end
    end

    class UnknownCommand < BadRequest
      def initialize(command, dataset)
        super("The command #{command.inspect} is not available " +
                "for the dataset #{dataset.inspect}.")
      end
    end

    def initialize(options)
      @options = options
      @name = @options[:name]
      @sessions = {}
      @current_id = 0
      @local = Regexp.new("^#{@name}")
      @input_adapter =
        InputAdapter.new(self, :plugins => Droonga.catalog.option("plugins"))
      @output_adapter =
        OutputAdapter.new(self, :plugins => Droonga.catalog.option("plugins"))
      @loop = EventLoop.new
      @farm = Farm.new(name, @loop, :dispatcher => self)
      @forwarder = Forwarder.new(@loop)
      @replier = Replier.new(@forwarder)
      @distributor = Distributor.new(self, @options)
      @collector = Collector.new
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
      @collector.shutdown
      @input_adapter.shutdown
      @output_adapter.shutdown
      @farm.shutdown
      @loop.stop
      @loop_thread.join
    end

    def process_message(message)
      @message = message
      if message["type"] == "dispatcher"
        process_internal_message(message["body"])
      else
        begin
          assert_valid_message
          process_input_message(message)
        rescue MessageProcessingError => error
          reply("statusCode" => error.status_code,
                "body"       => error.response_body)
        end
      end
    end

    def forward(message, destination)
      $log.trace("#{log_tag}: forward start")
      @forwarder.forward(message, destination)
      $log.trace("#{log_tag}: forward done")
    end

    # Replies response to replyTo.
    #
    # @param [Hash] message
    #   The message to be replied. See {Replier#reply} for available keys.
    #
    #   The key-value pairs in request message are used as the default
    #   key-value pairs. For example, if the passed message doesn't
    #   include `id` key, `id` key's value is used in request message.
    #
    # @return [void]
    #
    # @see Replier#reply
    def reply(message)
      adapted_message = @output_adapter.adapt(@message.merge(message))
      return if adapted_message["replyTo"].nil?
      @replier.reply(adapted_message)
    end

    def process_internal_message(message)
      id = message["id"]
      session = @sessions[id]
      if session
        session.receive(message["input"], message["value"])
      else
        components = message["components"]
        if components
          planner = Planner.new(self, components)
          session = planner.create_session(id, @collector)
          @sessions[id] = session
        else
          #todo: take cases receiving result before its query into account
        end
        session.start
      end
      @sessions.delete(id) if session.done?
    end

    def dispatch(message, destination)
      if local?(destination)
        process_internal_message(message)
      else
        @forwarder.forward(@message.merge("body" => message),
                           "type" => "dispatcher",
                           "to"   => destination)
      end
    end

    def dispatch_components(components)
      id = generate_id
      destinations = {}
      components.each do |component|
        dataset = component["dataset"]
        if dataset
          routes = Droonga.catalog.get_routes(dataset, component)
          component["routes"] = routes
        else
          component["routes"] ||= [id]
        end
        routes = component["routes"]
        routes.each do |route|
          destinations[farm_path(route)] = true
        end
      end
      dispatch_message = { "id" => id, "components" => components }
      destinations.each_key do |destination|
        dispatch(dispatch_message, destination)
      end
    end

    def process_local_message(local_message)
      task = local_message["task"]
      partition_name = task["route"]
      component = task["component"]
      command = component["command"]
      descendants = {}
      component["descendants"].each do |name, routes|
        descendants[name] = routes.collect do |route|
          farm_path(route)
        end
      end
      local_message["descendants"] = descendants
      farm_message = @message.merge("body" => local_message,
                                    "type" => command)
      @farm.process(partition_name, farm_message)
    end

    def local?(route)
      route =~ @local
    end

    private
    def generate_id
      id = @current_id
      @current_id = id.succ
      return [@name, id].join('.#')
    end

    def farm_path(route)
      if route =~ /\A.*:\d+\/[^\.]+/
        $MATCH
      else
        route
      end
    end

    def process_input_message(message)
      adapted_message = @input_adapter.adapt(message)
      @distributor.process(adapted_message["type"], adapted_message)
    rescue Droonga::Catalog::UnknownDataset => error
      raise UnknownDataset.new(error.dataset)
    rescue Droonga::Pluggable::UnknownPlugin => error
      raise UnknownCommand.new(error.command, message["dataset"])
    end

    def assert_valid_message
      raise MissingDatasetParameter.new unless @message.include?("dataset")
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

      def create_session(id, collector)
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
        Session.new(id, @dispatcher, collector, tasks, inputs)
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
          descendants = {}
          (component["outputs"] || []).each do |output|
            descendants[output] = []
            @descendants[output].each do |index|
              @components[index]["n_of_expects"] += component["routes"].size
              descendants[output].concat(@components[index]["routes"])
            end
          end
          component["descendants"] = descendants
        end
      end
    end
  end
end
