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
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

require "optparse"
require "socket"
require "ipaddr"
require "fileutils"

require "coolio"

require "droonga/path"
require "droonga/serf"
require "droonga/service_control_protocol"

module Droonga
  module Command
    class DroongaEngine
      class << self
        def run(command_line_arguments)
          new.run(command_line_arguments)
        end
      end

      def initialize
        @configuration = Configuration.new
        @log_output = nil
      end

      def run(command_line_arguments)
        parse_command_line_arguments!(command_line_arguments)

        ensure_path

        if @configuration.daemon?
          Process.daemon
        end

        open_log_file do
          write_pid_file do
            run_main_loop
          end
        end
      end

      private
      def parse_command_line_arguments!(command_line_arguments)
        parser = OptionParser.new
        @configuration.add_command_line_options(parser)
        parser.parse!(command_line_arguments)
      end

      def ensure_path
        Path.base
      end

      def run_main_loop
        main_loop = MainLoop.new(@configuration)
        main_loop.run
      end

      def open_log_file
        if @configuration.log_file
          File.open(@configuration.log_file, "a") do |file|
            @log_output = file
            yield
          end
        else
          yield
        end
      end

      def write_pid_file
        if @configuration.pid_file
          File.open(@configuration.pid_file, "w") do |file|
            file.puts(Process.pid)
          end
          begin
            yield
          ensure
            FileUtils.rm_f(@configuration.pid_file)
          end
        else
          yield
        end
      end

      class Configuration
        DEFAULT_HOST = Socket.gethostname
        DEFAULT_PORT = 10031

        attr_reader :host, :port, :tag, :log_file, :pid_file
        def initialize
          @host = DEFAULT_HOST
          @port = DEFAULT_PORT
          @tag = "droonga"
          @log_file = nil
          @daemon = false
          @pid_file = nil
        end

        def engine_name
          "#{@host}:#{@port}/#{@tag}"
        end

        def address_family
          ip_address = IPAddr.new(IPSocket.getaddress(@host))
          ip_address.family
        end

        def log_level
          ENV["DROONGA_LOG_LEVEL"] || Logger::Level.default_label
        end

        def daemon?
          @daemon
        end

        def to_command_line
          command_line_options = [
            "--engine-name", engine_name,
          ]
          command_line_options
        end

        def add_command_line_options(parser)
          add_connection_options(parser)
          add_log_options(parser)
          add_process_options(parser)
          add_path_options(parser)
        end

        def listen_socket
          @listen_socket ||= TCPServer.new(@host, @port)
        end

        def heartbeat_socket
          @heartbeat_socket ||= bind_heartbeat_socket
        end

        private
        def add_connection_options(parser)
          parser.separator("")
          parser.separator("Connection:")
          parser.on("--host=HOST",
                    "The host name of the Droonga engine",
                    "(#{@host})") do |host|
            @host = host
          end
          parser.on("--port=PORT", Integer,
                    "The port number of the Droonga engine",
                    "(#{@port})") do |port|
            @port = port
          end
          parser.on("--tag=TAG",
                    "The tag of the Droonga engine",
                    "(#{@tag})") do |tag|
            @tag = tag
          end
        end

        def add_log_options(parser)
          parser.separator("")
          parser.separator("Log:")
          levels = Logger::Level::LABELS
          levels_label = levels.join(",")
          parser.on("--log-level=LEVEL", levels,
                    "The log level of the Droonga engine",
                    "[#{levels_label}]",
                    "(#{log_level})") do |level|
            ENV["DROONGA_LOG_LEVEL"] = level
          end
          parser.on("--log-file=FILE",
                    "Output logs to FILE") do |file|
            @log_file = file
          end
        end

        def add_process_options(parser)
          parser.separator("")
          parser.separator("Process:")
          parser.on("--daemon",
                    "Run as a daemon") do
            @daemon = true
          end
          parser.on("--pid-file=FILE",
                    "Put PID to the FILE") do |file|
            @pid_file = file
          end
        end

        def add_path_options(parser)
          parser.separator("")
          parser.separator("Path:")
          parser.on("--base-dir=DIR",
                    "Use DIR as the base directory",
                    "(#{Path.base})") do |dir|
            Path.base = dir
          end
        end

        def bind_heartbeat_socket
          socket = UDPSocket.new(address_family)
          socket.bind(@host, @port)
          socket
        end
      end

      class MainLoop
        def initialize(configuration)
          @configuration = configuration
          @loop = Coolio::Loop.default
        end

        def run
          @serf = run_serf
          @service_runner = run_service
          @loop_breaker = Coolio::AsyncWatcher.new
          @loop.attach(@loop_breaker)

          trap_signals
          @loop.run
          @serf.shutdown if @serf.running?

          @service_runner.success?
        end

        private
        def trap_signals
          trap(:TERM) do
            stop_gracefully
            trap(:TERM, "DEFAULT")
          end
          trap(:INT) do
            stop_immediately
            trap(:INT, "DEFAULT")
          end
          trap(:QUIT) do
            stop_immediately
            trap(:QUIT, "DEFAULT")
          end
          trap(:USR1) do
            restart_graceful
          end
          trap(:HUP) do
            restart_immediately
          end
        end

        def stop_gracefully
          @loop_breaker.signal
          @loop_breaker.detach
          @serf.shutdown
          @service_runner.stop_gracefully
        end

        def stop_immediately
          @loop_breaker.signal
          @loop_breaker.detach
          @serf.shutdown
          @service_runner.stop_immediately
        end

        def restart_graceful
          @loop_breaker.signal
          old_service_runner = @service_runner
          @service_runner = run_service
          @service_runner.on_ready = lambda do
            old_service_runner.stop_gracefully
          end
        end

        def restart_immediately
          @loop_breaker.signal
          old_service_runner = @service_runner
          @service_runner = run_service
          old_service_runner.stop_immediately
        end

        def run_service
          service_runner = ServiceRunner.new(@loop, @configuration)
          service_runner.run
          service_runner
        end

        def run_serf
          serf = Serf.new(@loop, @configuration.engine_name)
          serf.start
          serf
        end
      end

      class ServiceRunner
        include ServiceControlProtocol

        def initialize(raw_loop, configuration)
          @raw_loop = raw_loop
          @configuration = configuration
          @success = false
        end

        def on_ready=(callback)
          @on_ready = callback
        end

        def run
          control_write_in, control_write_out = IO.pipe
          control_read_in, control_read_out = IO.pipe
          listen_fd = @configuration.listen_socket.fileno
          heartbeat_fd = @configuration.heartbeat_socket.fileno
          env = {}
          command_line = [
            RbConfig.ruby,
            "-S",
            "#{$0}-service",
            "--listen-fd", listen_fd.to_s,
            "--heartbeat-fd", heartbeat_fd.to_s,
            "--control-read-fd", control_write_in.fileno.to_s,
            "--control-write-fd", control_read_out.fileno.to_s,
            *@configuration.to_command_line,
          ]
          options = {
            listen_fd => listen_fd,
            heartbeat_fd => heartbeat_fd,
            control_write_in => control_write_in,
            control_read_out => control_read_out,
          }
          if @log_output
            options[:out] = @log_output
            options[:err] = @log_output
          end
          @pid = spawn(env, *command_line, options)
          control_write_in.close
          control_read_out.close
          attach_control_write_out(control_write_out)
          attach_control_read_in(control_read_in)
        end

        def stop_gracefully
          @control_write_out.write(Messages::STOP_GRACEFUL)
        end

        def stop_immediately
          @control_write_out.write(Messages::STOP_IMMEDIATELY)
        end

        def success?
          @success
        end

        private
        def on_ready
          @on_ready.call if @on_ready
        end

        def on_finish
          _, status = Process.waitpid2(@pid)
          @success = status.success?
          @control_write_out.close
          @control_read_in.close
        end

        def attach_control_write_out(control_write_out)
          @control_write_out = Coolio::IO.new(control_write_out)
          @raw_loop.attach(@control_write_out)
        end

        def attach_control_read_in(control_read_in)
          @control_read_in = Coolio::IO.new(control_read_in)
          on_read = lambda do |data|
            # TODO: should buffer data to handle half line received case
            data.each_line do |line|
              case line
              when Messages::READY
                on_ready
              when Messages::FINISH
                on_finish
              end
            end
          end
          @control_read_in.on_read do |data|
            on_read.call(data)
          end
          @raw_loop.attach(@control_read_in)
        end
      end
    end
  end
end
