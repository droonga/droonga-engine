# Copyright (C) 2013-2014 Droonga Project
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

require "droonga/adapter_runner"
require "droonga/planner_runner"
require "droonga/collector_runner"
require "droonga/farm"
require "droonga/session"
require "droonga/replier"
require "droonga/error_messages"
require "droonga/distributor"

module Droonga
  class Dispatcher
    attr_reader :name

    class MissingDatasetParameter < ErrorMessages::BadRequest
      def initialize
        super("Missing required parameter: <dataset>")
      end
    end

    class UnknownDataset < ErrorMessages::NotFound
      def initialize(dataset)
        super("Unknown dataset: <#{dataset}>")
      end
    end

    class UnknownType < ErrorMessages::BadRequest
      def initialize(type, dataset)
        super("[#{dataset}] Handler not found for the type: <#{type}>")
      end
    end

    def initialize(catalog, options)
      @catalog = catalog
      @options = options
      @name = @options[:name]
      @loop = EventLoop.new
      @sessions = {}
      @current_id = 0
      @local = Regexp.new("^#{@name}")
      @adapter_runners = create_adapter_runners
      @farm = Farm.new(name, @catalog, @loop, :dispatcher => self)
      @forwarder = Forwarder.new(@loop)
      @replier = Replier.new(@forwarder)
      @planner_runners = create_planner_runners
      @collector_runners = create_collector_runners
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
      @planner_runners.each_value do |planner_runner|
        planner_runner.shutdown
      end
      @collector_runners.each_value do |collector_runner|
        collector_runner.shutdown
      end
      @adapter_runners.each_value do |adapter_runner|
        adapter_runner.shutdown
      end
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
          assert_valid_message(message)
          process_input_message(message)
        rescue ErrorMessage => error
          reply("statusCode" => error.status_code,
                "body"       => error.response_body)
        rescue StandardError, LoadError, SyntaxError => error
          Logger.error("failed to process input message", error)
          formatted_error = ErrorMessages::InternalServerError.new("Unknown internal error")
          reply("statusCode" => formatted_error.status_code,
                "body"       => formatted_error.response_body)
          raise error
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
      adapted_message = @message.merge(message)
      adapter_runner = @adapter_runners[adapted_message["dataset"]]
      if adapter_runner
        adapted_message = adapter_runner.adapt_output(adapted_message)
      end
      return if adapted_message["replyTo"].nil?
      @replier.reply(adapted_message)
    end

    def process_internal_message(message)
      id = message["id"]
      session = @sessions[id]
      if session
        session.receive(message["input"], message["value"])
      else
        steps = message["steps"]
        if steps
          session_planner = SessionPlanner.new(self, steps)
          dataset = message["dataset"] || @message["dataset"]
          collector_runner = @collector_runners[dataset]
          session = session_planner.create_session(id, collector_runner)
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

    def dispatch_steps(steps)
      id = generate_id
      destinations = {}
      steps.each do |step|
        dataset = step["dataset"]
        if dataset
          routes = @catalog.get_routes(dataset, step)
          step["routes"] = routes
        else
          step["routes"] ||= [id]
        end
        routes = step["routes"]
        routes.each do |route|
          destinations[farm_path(route)] = true
        end
      end
      dispatch_message = { "id" => id, "steps" => steps }
      destinations.each_key do |destination|
        dispatch(dispatch_message, destination)
      end
    end

    def process_local_message(local_message)
      task = local_message["task"]
      slice_name = task["route"]
      step = task["step"]
      command = step["command"]
      descendants = {}
      step["descendants"].each do |name, routes|
        descendants[name] = routes.collect do |route|
          farm_path(route)
        end
      end
      local_message["descendants"] = descendants
      farm_message = @message.merge("body" => local_message,
                                    "type" => command)
      @farm.process(slice_name, farm_message)
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
      dataset = message["dataset"]
      adapter_runner = @adapter_runners[dataset]
      adapted_message = adapter_runner.adapt_input(message)
      planner_runner = @planner_runners[dataset]
      plan = planner_runner.plan(adapted_message)
      distributor = Distributor.new(self)
      distributor.distribute(plan)
    rescue Droonga::UnsupportedMessageError => error
      target_message = error.message
      raise UnknownType.new(target_message["type"], target_message["dataset"])
    end

    def assert_valid_message(message)
      unless message.key?("dataset")
        raise MissingDatasetParameter.new
      end
      dataset = message["dataset"]
      unless @catalog.have_dataset?(dataset)
        raise UnknownDataset.new(dataset)
      end
    end

    def create_runners
      runners = {}
      @catalog.datasets.each do |name, configuration|
        runners[name] = yield(configuration)
      end
      runners
    end

    def create_adapter_runners
      create_runners do |configuration|
        AdapterRunner.new(self, configuration["plugins"] || [])
      end
    end

    def create_planner_runners
      create_runners do |configuration|
        PlannerRunner.new(self, configuration["plugins"] || [])
      end
    end

    def create_collector_runners
      create_runners do |configuration|
        CollectorRunner.new(configuration["plugins"] || [])
      end
    end

    def log_tag
      "[#{Process.ppid}][#{Process.pid}] dispatcher"
    end

    class SessionPlanner
      attr_reader :steps

      def initialize(dispatcher, steps)
        @dispatcher = dispatcher
        @steps = steps
      end

      def create_session(id, collector_runner)
        resolve_descendants
        tasks = []
        inputs = {}
        @steps.each do |step|
          step["routes"].each do |route|
            next unless @dispatcher.local?(route)
            task = {
              "route" => route,
              "step" => step,
              "n_of_inputs" => 0,
              "values" => {}
            }
            tasks << task
            (step["inputs"] || [nil]).each do |input|
              inputs[input] ||= []
              inputs[input] << task
            end
          end
        end
        Session.new(id, @dispatcher, collector_runner, tasks, inputs)
      end

      def resolve_descendants
        @descendants = {}
        @steps.size.times do |index|
          step = @steps[index]
          (step["inputs"] || []).each do |input|
            @descendants[input] ||= []
            @descendants[input] << index
          end
          step["n_of_expects"] = 0
        end
        @steps.each do |step|
          descendants = {}
          (step["outputs"] || []).each do |output|
            descendants[output] = []
            @descendants[output].each do |index|
              @steps[index]["n_of_expects"] += step["routes"].size
              descendants[output].concat(@steps[index]["routes"])
            end
          end
          step["descendants"] = descendants
        end
      end
    end
  end
end
