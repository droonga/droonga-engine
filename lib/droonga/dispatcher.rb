# Copyright (C) 2013-2015 Droonga Project
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

    attr_reader :engine_state, :cluster

    def initialize(engine_state, cluster, catalog)
      @engine_state = engine_state
      @cluster = cluster
      @forwarder = @engine_state.forwarder
      @replier = @engine_state.replier
      @catalog = catalog
      @adapter_runners = create_adapter_runners
      @farm = Farm.new(@engine_state.name, @catalog, @engine_state.loop,
                       :engine_state => @engine_state,
                       :cluster => @cluster,
                       :dispatcher => self,
                       :forwarder  => @forwarder)
      @engine_state.wait_until_ready(@farm)
      @collector_runners = create_collector_runners
      @step_runners = create_step_runners
    end

    def start
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
      @farm.stop_immediately
      logger.trace("stop_immediately: done")
    end

    def process_message(message)
      logger.trace("process_message: start", :message => message)
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
      logger.trace("process_message: done")
    end

    def forward(message, destination)
      logger.trace("forward start")
      if local_route?(destination) or direct_route?(destination)
        @forwarder.forward(message, destination)
      else
        @cluster.forward(message, destination)
      end
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
      logger.trace("process_internal_message: start", :message => message)
      id = message["id"]
      session = @engine_state.find_session(id)
      if session
        session.receive(message["input"], message["value"])
      else
        logger.trace("process_internal_message: no session")
        steps = message["steps"]
        if steps
          session_planner = SessionPlanner.new(@engine_state, @cluster, steps)
          dataset = message["dataset"] || @message["dataset"]
          collector_runner = @collector_runners[dataset]
          session = session_planner.create_session(id, self, collector_runner)
          if session.need_result?
            timeout_seconds = message["timeout_seconds"] || nil
            @engine_state.register_session(id, session,
                                           :timeout_seconds => timeout_seconds)
            session.start
            logger.trace("process_internal_message: waiting for results")
          else
            session.start
            session.finish
            session = nil
            logger.trace("process_internal_message: no need to wait for results")
          end
        else
          logger.error("no steps error", :id => id, :message => message)
          return
          #todo: take cases receiving result before its query into account
        end
      end
      @engine_state.unregister_session(id) if session and session.done?
      logger.trace("process_internal_message: done")
    end

    def dispatch(message, destination)
      logger.trace("dispatch: start", :message => message, :destination => destination)
      if local_route?(destination)
        process_internal_message(message)
      else
        forward_message = @message.merge("body" => message)
        forward_destination = {
          "type" => "dispatcher",
          "to"   => destination,
        }
        if direct_route?(forward_destination)
          @forwarder.forward(forward_message, forward_destination)
        else
          @cluster.forward(forward_message, forward_destination)
        end
      end
      logger.trace("dispatch: done")
    end

    def dispatch_steps(steps)
      logger.trace("dispatch_steps: start", :steps => steps)
      id = @engine_state.generate_id

      destinations = []
      timeout_seconds = nil
      steps.each do |step|
        calculated_timeout_seconds = timeout_seconds_from_step(step)
        if calculated_timeout_seconds
          timeout_seconds = calculated_timeout_seconds
        end

        dataset = @catalog.dataset(step["dataset"])
        if dataset
          if write_step?(step)
            step["write"] = true
            target_nodes = @cluster.writable_nodes
            if target_nodes.empty?
              logger.error("there is no node to dispath a write step!",
                           :my_role   => @engine_state.role,
                           :all_nodes => @cluster.engine_nodes.collect(&:to_json),
                           :step      => step)
            end
          else
            target_nodes = @cluster.readable_nodes
            if target_nodes.empty?
              logger.error("there is no node to dispath a read step!",
                           :my_role   => @engine_state.role,
                           :all_nodes => @cluster.engine_nodes.collect(&:to_json),
                           :step      => step)
            end
          end
          routes = dataset.compute_routes(step, target_nodes)
          step["routes"] = routes.collect do |route|
            internal_route(route)
          end
        else
          step["routes"] ||= [id]
        end

        destinations += step["routes"].collect do |route|
          internal_farm_path(route)
        end
      end

      dispatch_message = {
        "id"    => id,
        "steps" => steps,
        "timeout_seconds" => timeout_seconds,
      }
      destinations.uniq.each do |destination|
        dispatch(dispatch_message, destination)
      end

      logger.trace("dispatch_steps: done")
    end

    def process_local_message(local_message)
      logger.trace("process_local_message: start", :steps => local_message)
      task = local_message["task"]
      slice_name = task["route"]
      slice_name = public_route(slice_name)
      step = task["step"]
      command = step["command"]
      descendants = {}
      step["descendants"].each do |name, routes|
        descendants[name] = routes.collect do |route|
          internal_farm_path(route)
        end
      end
      local_message["descendants"] = descendants
      farm_message = @message.merge("body" => local_message,
                                    "type" => command)
      @farm.process(slice_name, farm_message)
      logger.trace("process_local_message: done")
    end

    def local_route?(route)
      @engine_state.local_route?(route)
    end

    def direct_route?(route)
      receiver = route["to"]
      not @cluster.engine_node_names.include?(receiver)
    end

    def write_step?(step)
      return false unless step["dataset"]

      step_runner = @step_runners[step["dataset"]]
      return false unless step_runner

      step_definition = step_runner.find(step["command"])
      return false unless step_definition

      step_definition.write?
    end

    def timeout_seconds_from_step(step)
      return nil unless step["dataset"]

      step_runner = @step_runners[step["dataset"]]
      return nil unless step_runner

      step_definition = step_runner.find(step["command"])
      return nil unless step_definition

      step_definition.timeout_seconds_for_step(step)
    end

    private
    def internal_route(route)
      @engine_state.internal_route(route)
    end

    def public_route(route)
      @engine_state.public_route(route)
    end

    def internal_farm_path(route)
      @engine_state.internal_farm_path(route)
    end

    def public_farm_path(route)
      @engine_state.public_farm_path(route)
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
      logger.trace("process_input_message: rescue", :error => error)
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

      def initialize(engine_state, cluster, steps)
        @engine_state = engine_state
        @cluster = cluster
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
              responsive_routes = select_responsive_routes(step["routes"])
              @steps[index]["n_of_expects"] += responsive_routes.size
              descendants[output].concat(@steps[index]["routes"])
            end
          end
          step["descendants"] = descendants
        end
      end

      def select_responsive_routes(routes)
        selected_nodes = @cluster.readable_nodes
        routes.select do |route|
          selected_nodes.include?(@engine_state.public_farm_path(route))
        end
      end
    end
  end
end
