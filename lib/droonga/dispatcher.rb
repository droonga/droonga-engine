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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "tsort"

require "droonga/loggable"
require "droonga/adapter_runner"
require "droonga/collector_runner"
require "droonga/step_runner"
require "droonga/farm"
require "droonga/session"
require "droonga/error_messages"
require "droonga/distributor"

module Droonga
  class Dispatcher
    include Loggable

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

    attr_reader :engine_state

    def initialize(engine_state, catalog)
      @engine_state = engine_state
      @forwarder = @engine_state.forwarder
      @replier = @engine_state.replier
      @catalog = catalog
      @adapter_runners = create_adapter_runners
      @farm = Farm.new(@engine_state.name, @catalog, @engine_state.loop,
                       :engine_state => @engine_state,
                       :dispatcher => self,
                       :forwarder  => @forwarder)
      @collector_runners = create_collector_runners
      @step_runners = create_step_runners
    end

    def start
      @farm.on_ready = lambda do
        @engine_state.on_ready
      end
      @farm.start
    end

    def stop_gracefully(&on_stop)
      logger.trace("stop_gracefully: start")
      @collector_runners.each_value do |collector_runner|
        collector_runner.shutdown
      end
      @adapter_runners.each_value do |adapter_runner|
        adapter_runner.shutdown
      end
      @farm.stop_gracefully(&on_stop)
      logger.trace("stop_gracefully: done")
    end

    def stop_immediately
      logger.trace("stop_immediately: start")
      @collector_runners.each_value do |collector_runner|
        collector_runner.shutdown
      end
      @adapter_runners.each_value do |adapter_runner|
        adapter_runner.shutdown
      end
      @farm.shutdown
      logger.trace("stop_immediately: done")
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
          logger.exception("failed to process input message", error)
          formatted_error = ErrorMessages::InternalServerError.new("Unknown internal error")
          reply("statusCode" => formatted_error.status_code,
                "body"       => formatted_error.response_body)
        end
      end
    end

    def forward(message, destination)
      logger.trace("forward start")
      @forwarder.forward(message, destination)
      logger.trace("forward done")
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
      if adapted_message["replyTo"].nil?
        status_code = adapted_message["statusCode"] || 200
        if status_code != 200
          dataset = adapted_message["dataset"]
          body = adapted_message["body"] || {}
          name = body["name"] || "Unknown"
          message = body["message"] || "unknown error"
          logger.error("orphan error: " +
                         "<#{dataset}>[#{name}](#{status_code}): #{message}")
        end
      else
        @replier.reply(adapted_message)
      end
    end

    def process_internal_message(message)
      id = message["id"]
      session = @engine_state.find_session(id)
      if session
        session.receive(message["input"], message["value"])
      else
        steps = message["steps"]
        if steps
          session_planner = SessionPlanner.new(@engine_state, steps)
          dataset = message["dataset"] || @message["dataset"]
          collector_runner = @collector_runners[dataset]
          session = session_planner.create_session(id, self, collector_runner)
          @engine_state.register_session(id, session)
        else
          logger.error("no steps error: id=#{id}, message=#{message}")
          return
          #todo: take cases receiving result before its query into account
        end
        session.start
      end
      @engine_state.unregister_session(id) if session.done?
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
      id = @engine_state.generate_id

      destinations = []
      steps.each do |step|
        dataset = @catalog.dataset(step["dataset"])
        if dataset
          if write_step?(step)
            target_nodes = @engine_state.live_nodes
          else
            target_nodes = @engine_state.active_nodes
          end
          routes = dataset.compute_routes(step, target_nodes)
          step["routes"] = routes
        else
          step["routes"] ||= [id]
        end
        destinations += step["routes"].collect do |route|
          farm_path(route)
        end
      end

      dispatch_message = { "id" => id, "steps" => steps }
      destinations.uniq.each do |destination|
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
      @engine_state.local_route?(route)
    end

    def write_step?(step)
      return false unless step["dataset"]

      step_runner = @step_runners[step["dataset"]]
      return false unless step_runner

      step_definition = step_runner.find(step["command"])
      return false unless step_definition

      step_definition.write?
    end

    private
    def farm_path(route)
      @engine_state.farm_path(route)
    end

    def process_input_message(message)
      dataset = message["dataset"]
      adapter_runner = @adapter_runners[dataset]
      adapted_message = adapter_runner.adapt_input(message)
      step_runner = @step_runners[dataset]
      plan = step_runner.plan(adapted_message)
      distributor = Distributor.new(self, plan)
      distributor.distribute
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
      @catalog.datasets.each do |name, dataset|
        runners[name] = yield(dataset)
      end
      runners
    end

    def create_adapter_runners
      create_runners do |dataset|
        AdapterRunner.new(self, dataset.plugins)
      end
    end

    def create_collector_runners
      create_runners do |dataset|
        CollectorRunner.new(dataset.plugins)
      end
    end

    def create_step_runners
      create_runners do |dataset|
        StepRunner.new(dataset, dataset.plugins)
      end
    end

    def log_tag
      "dispatcher"
    end

    class SessionPlanner
      attr_reader :steps

      def initialize(engine_state, steps)
        @engine_state = engine_state
        @steps = steps
      end

      def create_session(id, dispatcher, collector_runner)
        resolve_descendants
        tasks = []
        inputs = {}
        @steps.each do |step|
          step["routes"].each do |route|
            next unless @engine_state.local_route?(route)
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
        Session.new(id, dispatcher, collector_runner, tasks, inputs)
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
              active_routes = @engine_state.remove_inactive_routes(step["routes"])
              @steps[index]["n_of_expects"] += active_routes.size
              descendants[output].concat(@steps[index]["routes"])
            end
          end
          step["descendants"] = descendants
        end
      end
    end
  end
end
