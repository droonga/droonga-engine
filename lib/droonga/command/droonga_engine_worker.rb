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

require "optparse"
require "fileutils"

require "coolio"
require "sigdump/setup"

require "droonga/job_receiver"
require "droonga/plugin_loader"
require "droonga/worker_process_agent"
require "droonga/handler_runner"

module Droonga
  module Command
    class DroongaEngineWorker
      class << self
        def run(command_line_arguments)
          new.run(command_line_arguments)
        end
      end

      include Loggable

      def initialize
        @job_queue_socket_path = nil
        @contrtol_read_fd = nil
        @contrtol_write_fd = nil
        @pid_file_path = nil
        @dataset = nil
        @database_path = nil
        @plugins = []
        @internal_connection_lifetime = nil
        @worker_process_agent = nil
      end

      def run(command_line_arguments)
        create_new_process_group

        parse_command_line_arguments!(command_line_arguments)
        PluginLoader.load_all

        write_pid_file do
          run_main_loop
        end
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
        parser.on("--job-queue-socket-path=PATH",
                  "Read jobs from PATH") do |path|
          @job_queue_socket_path = Pathname.new(path)
        end
        parser.on("--control-read-fd=FD", Integer,
                  "Use FD to read control messages from the service") do |fd|
          @control_read_fd = fd
        end
        parser.on("--control-write-fd=FD", Integer,
                  "Use FD to write control messages from the service") do |fd|
          @control_write_fd = fd
        end
        parser.on("--pid-file=PATH",
                  "Put PID to PATH") do |path|
          @pid_file_path = Pathname.new(path)
        end
        parser.on("--dataset=DATASET",
                  "Process DATASET") do |dataset|
          @dataset = dataset
        end
        parser.on("--database-path=PATH",
                  "Use database at PATH") do |path|
          @database_path = Pathname.new(path)
        end
        parser.on("--plugins=PLUGIN1,PLUGIN2,...", Array,
                  "Use PLUGINs") do |plugins|
          @plugins = plugins
        end
        parser.on("--internal-connection-lifetime=SECONDS", Float,
                  "The time to expire internal connections, in seconds") do |seconds|
          @internal_connection_lifetime = seconds
        end
      end

      def write_pid_file
        if @pid_file_path
          @pid_file_path.open("w") do |file|
            file.puts(Process.pid)
          end
          begin
            yield
          ensure
            FileUtils.rm_f(@pid_file_path.to_s)
          end
        else
          yield
        end
      end

      def run_main_loop
        begin
          start
          true
        rescue
          logger.exception("failed while running", $!)
          false
        ensure
          stop_worker_process_agent
        end
      end

      def start
        @stopping = false
        @loop = Coolio::Loop.default

        start_forwarder
        start_handler_runner
        start_job_receiver
        start_worker_process_agent

        @loop.run
      end

      def stop_gracefully
        return if @stopping
        @stopping = true

        stop_worker_process_agent
        stop_job_receiver
        stop_handler_runner
        stop_forwarder
      end

      # It may be called after stop_gracefully.
      def stop_immediately
        stop_gracefully
        @loop.stop
      end

      def refresh_node_reference
        @forwarder.refresh_all_connections
      end

      def start_forwarder
        @forwarder = Forwarder.new(@loop,
                                   :auto_close_timeout =>
                                     @internal_connection_lifetime)
        @forwarder.start
      end

      def stop_forwarder
        @forwarder.shutdown
      end

      def start_handler_runner
        options = {
          :forwarder => @forwarder,
          :dataset   => @dataset,
          :database  => @database_path.to_s,
          :plugins   => @plugins,
        }
        @handler_runner = HandlerRunner.new(@loop, options)
        @handler_runner.start
      end

      def stop_handler_runner
        @handler_runner.shutdown
      end

      def start_job_receiver
        @job_receiver = create_job_receiver
        @job_receiver.start
      end

      def create_job_receiver
        JobReceiver.new(@loop, @job_queue_socket_path.to_s) do |message|
          process(message)
        end
      end

      def process(message)
        logger.trace("process: start")
        @handler_runner.process(message)
        logger.trace("process: done")
      end

      def stop_job_receiver
        @job_receiver.shutdown
      end

      def start_worker_process_agent
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
        @worker_process_agent.on_refresh_node_reference = lambda do
          refresh_node_reference
        end
        @worker_process_agent.start
        @worker_process_agent.ready
      end

      def stop_worker_process_agent
        return if @worker_process_agent.nil?
        @worker_process_agent.stop
      end

      def log_tag
        "[#{Process.ppid}] worker"
      end
    end
  end
end
