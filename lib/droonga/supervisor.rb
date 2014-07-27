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

require "droonga/loggable"
require "droonga/process_supervisor"

module Droonga
  class Supervisor
    include Loggable

    attr_writer :on_ready
    def initialize(loop, n_workers, config)
      @loop = loop
      @n_workers = n_workers
      @config = config
      @on_ready = nil
    end

    def start
      n_ready_workers = 0
      @worker_runners = @n_workers.times.collect do |i|
        worker_runner = WorkerRunner.new(@loop, i, @config)
        worker_runner.on_ready = lambda do
          n_ready_workers += 1
          if n_ready_workers == @n_workers
            @on_ready.call if @on_ready
          end
        end
        worker_runner.start
        # TODO: support auto re-run
        worker_runner
      end
    end

    def stop_gracefully
      @worker_runners.each do |worker_runner|
        worker_runner.stop_gracefully
      end
    end

    def stop_immediately
      @worker_runners.each do |worker_runner|
        worker_runner.stop_immediately
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
      def initialize
        @name = nil
        @dataset = nil
        @database_path = nil
        @plugins = []
        @job_pusher = nil
      end
    end

    class WorkerRunner
      include Loggable

      attr_writer :on_ready
      attr_writer :on_failure
      def initialize(loop, id, config)
        @loop = loop
        @id = id
        @config = config
        @on_ready = nil
        @on_failure = nil
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

      def stop_gracefully
        @supervisor.stop_gracefully
      end

      def stop_immediately
        @supervisor.stop_immediately
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

      def on_ready
        @on_ready.call if @on_ready
      end

      def on_failure
        # TODO: log
        @on_failure.call if @on_failure
      end

      def on_finish
        _, status = Process.waitpid2(@pid)
        @success = status.success?
        @supervisor.stop
        on_failure unless success?
      end

      private
      def log_tag
        "worker-runner"
      end
    end
  end
end
