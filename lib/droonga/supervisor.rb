# Copyright (C) 2014-2015 Droonga Project
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

require "droonga/loggable"
require "droonga/deferrable"
require "droonga/process_supervisor"

module Droonga
  class Supervisor
    include Loggable
    include Deferrable

    def initialize(loop, n_workers, config)
      @loop = loop
      @n_workers = n_workers
      @config = config
    end

    def start
      n_ready_workers = 0
      @worker_runners = @n_workers.times.collect do |i|
        worker_runner = WorkerRunner.new(@loop, i, @config)
        worker_runner.on_ready = lambda do
          n_ready_workers += 1
          if n_ready_workers == @n_workers
            on_ready
          end
        end
        worker_runner.start
        # TODO: support auto re-run
        worker_runner
      end
    end

    def stop_gracefully
      logger.trace("stop_gracefully: start")
      n_worker_runners = @worker_runners.size
      n_done_worker_runners = 0
      @worker_runners.each do |worker_runner|
        worker_runner.stop_gracefully do
          n_done_worker_runners += 1
          if n_done_worker_runners == n_worker_runners
            yield if block_given?
            logger.trace("stop_gracefully: done")
          end
        end
      end
    end

    def stop_immediately
      @worker_runners.each do |worker_runner|
        worker_runner.stop_immediately
      end
    end

    def refresh_node_reference
      @worker_runners.each do |worker_runner|
        worker_runner.refresh_node_reference
      end
    end

    private
    def log_tag
      "supervisor"
    end

    class WorkerConfiguration
      attr_accessor :name
      attr_accessor :dataset
      attr_accessor :database_path
      attr_accessor :plugins
      attr_accessor :job_pusher
      attr_accessor :internal_connection_lifetime
      def initialize
        @name = nil
        @dataset = nil
        @database_path = nil
        @plugins = []
        @job_pusher = nil
        @internal_connection_lifetime = nil
      end
    end

    class WorkerRunner
      include Loggable
      include Deferrable

      def initialize(loop, id, config)
        @loop = loop
        @id = id
        @config = config
        @stop_gracefully_callback = nil
      end

      def start
        control_write_in, control_write_out = IO.pipe
        control_read_in, control_read_out = IO.pipe
        env = {}
        command_line = [
          RbConfig.ruby,
          "-S",
          "droonga-engine-worker",
          "--control-read-fd", control_write_in.fileno.to_s,
          "--control-write-fd", control_read_out.fileno.to_s,
          "--job-queue-socket-path", @config.job_pusher.socket_path.to_s,
          "--pid-file", pid_path.to_s,
          "--dataset", @config.dataset.name,
          "--database-path", @config.database_path.to_s,
          "--plugins", @config.plugins.join(","),
          "--internal-connection-lifetime",
            @config.internal_connection_lifetime.to_s,
        ]
        options = {
          control_write_in => control_write_in,
          control_read_out => control_read_out,
        }
        @pid = spawn(env, *command_line, options)
        control_write_in.close
        control_read_out.close
        @supervisor = create_process_supervisor(control_read_in,
                                                control_write_out)
        @supervisor.start
      end

      def stop_gracefully(&block)
        logger.trace("stop_gracefully: start")
        @supervisor.stop_gracefully
        @stop_gracefully_callback = lambda do
          yield if block_given?
          logger.trace("stop_gracefully: done")
        end
      end

      def stop_immediately
        @supervisor.stop_immediately
      end

      def refresh_node_reference
        @supervisor.refresh_node_reference
      end

      def success?
        @success
      end

      private
      def pid_path
        @config.database_path + "droonga-worker-#{@id}.pid"
      end

      def create_process_supervisor(input, output)
        supervisor = ProcessSupervisor.new(@loop, input, output)
        supervisor.on_ready = lambda do
          on_ready
        end
        supervisor.on_finish = lambda do
          on_finish
        end
        supervisor
      end

      def on_finish
        _, status = Process.waitpid2(@pid)
        @success = status.success?
        @supervisor.stop
        on_failure unless success?
        @stop_gracefully_callback.call if @stop_gracefully_callback
      end

      private
      def log_tag
        "worker-runner"
      end
    end
  end
end
