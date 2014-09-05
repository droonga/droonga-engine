# Copyright (C) 2014 Droonga Project
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

require "optparse"

require "coolio"

require "droonga/worker_process_agent"
require "droonga/engine"
require "droonga/fluent_message_receiver"
require "droonga/internal_fluent_message_receiver"
require "droonga/plugin_loader"

module Droonga
  module Command
    class DroongaEngineService
      class << self
        def run(command_line_arguments)
          new.run(command_line_arguments)
        end
      end

      include Loggable

      def initialize
        @engine_name = nil
        @listen_fd = nil
        @heartbeat_fd = nil
        @contrtol_read_fd = nil
        @contrtol_write_fd = nil
        @contrtol_write_closed = false
      end

      def run(command_line_arguments)
        create_new_process_group

        parse_command_line_arguments!(command_line_arguments)
        PluginLoader.load_all

        control_write_io = IO.new(@control_write_fd)
        success = true
        begin
          run_services
        rescue
          logger.exception("failed to run services", $!)
          success = false
        ensure
          shutdown_worker_process_agent
        end

        success
      end

      private
      def create_new_process_group
        begin
          Process.setsid
        rescue SystemCallError, NotImplementedError
        end
      end

      def parse_command_line_arguments!(command_line_arguments)
        parser = OptionParser.new
        add_internal_options(parser)
        parser.parse!(command_line_arguments)
      end

      def add_internal_options(parser)
        parser.separator("")
        parser.separator("Internal:")
        parser.on("--engine-name=NAME",
                  "Use NAME as the name of the engine") do |name|
          @engine_name = name
        end
        parser.on("--listen-fd=FD", Integer,
                  "Use FD as the listen file descriptor") do |fd|
          @listen_fd = fd
        end
        parser.on("--heartbeat-fd=FD", Integer,
                  "Use FD as the heartbeat file descriptor") do |fd|
          @heartbeat_fd = fd
        end
        parser.on("--control-read-fd=FD", Integer,
                  "Use FD to read control messages from the service") do |fd|
          @control_read_fd = fd
        end
        parser.on("--control-write-fd=FD", Integer,
                  "Use FD to write control messages from the service") do |fd|
          @control_write_fd = fd
        end
      end

      def host
        @engine_name.split(":", 2).first
      end

      def run_services
        @stopping = false
        @engine = nil
        @receiver = nil
        @loop = Coolio::Loop.default

        run_internal_message_receiver
        run_worker_process_agent
        run_engine
        run_receiver
        @loop.run
      end

      def run_internal_message_receiver
        @internal_message_receiver = create_internal_message_receiver
        host, port = @internal_message_receiver.start
        tag = @engine_name.split("/", 2).last.split(".", 2).first
        @internal_engine_name = "#{host}:#{port}/#{tag}"
      end

      def create_internal_message_receiver
        InternalFluentMessageReceiver.new(@loop, host) do |tag, time, record|
          on_message(tag, time, record)
        end
      end

      def shutdown_internal_message_receiver
        if @internal_message_receiver.nil?
          yield
          return
        end
        @internal_message_receiver, receiver = nil, @internal_message_receiver
        receiver.shutdown do
          yield
        end
      end

      def run_engine
        @engine = Engine.new(@loop, @engine_name, @internal_engine_name)
        @engine.on_ready = lambda do
          @worker_process_agent.ready
        end
        @engine.start
      end

      def run_receiver
        @receiver = create_receiver
        @receiver.start
      end

      def run_worker_process_agent
        input = IO.new(@control_read_fd)
        @control_read_fd = nil
        output = IO.new(@control_write_fd)
        @control_write_fd = nil
        @worker_process_agent = WorkerProcessAgent.new(@loop, input, output)
        @worker_process_agent.on_stop_gracefully = lambda do
          stop_gracefully
        end
        @worker_process_agent.on_stop_immediately = lambda do
          stop_immediately
        end
        @worker_process_agent.start
      end

      def shutdown_worker_process_agent
        @worker_process_agent.stop do
          yield
        end
      end

      def create_receiver
        options = {
          :listen_fd => @listen_fd,
          :heartbeat_fd => @heartbeat_fd,
        }
        FluentMessageReceiver.new(@loop, options) do |tag, time, record|
          on_message(tag, time, record)
        end
      end

      def on_message(tag, time, record)
        prefix, type, *arguments = tag.split(/\./)
        if type.nil? or type.empty? or type == "message"
          message = record
        else
          message = {
            "type" => type,
            "arguments" => arguments,
            "body" => record
          }
        end
        reply_to = message["replyTo"]
        if reply_to.is_a? String
          message["replyTo"] = {
            "type" => "#{message["type"]}.result",
            "to" => reply_to
          }
        end

        @engine.process(message)
      end

      def stop_gracefully
        logger.trace("stop_gracefully: start")
        return if @stopping
        @stopping = true

        n_rest_shutdowns = 3
        on_finish = lambda do
          n_rest_shutdowns -= 1
          if n_rest_shutdowns.zero?
            yield
            logger.trace("stop_gracefully: done")
            logger.trace("watchers = #{@loop.watchers.inspect}")
          end
        end

        @receiver.stop_gracefully do
          on_finish.call
        end
        @engine.stop_gracefully do
          shutdown_worker_process_agent do
            on_finish.call
          end
          shutdown_internal_message_receiver do
            on_finish.call
          end
        end
      end

      # It may be called after stop_gracefully.
      def stop_immediately
        shutdown_worker_process_agent
        @receiver.stop_immediately
        shutdown_internal_message_receiver
        @engine.stop_immediately
        @loop.stop
      end

      def log_tag
        "droonga-engine-service"
      end
    end
  end
end
