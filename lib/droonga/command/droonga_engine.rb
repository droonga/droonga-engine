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

require "optparse"
require "socket"
require "ipaddr"
require "fileutils"
require "yaml"

require "coolio"
require "sigdump/setup"

require "droonga/engine/version"
require "droonga/path"
require "droonga/address"
require "droonga/serf"
require "droonga/node_metadata"
require "droonga/file_observer"
require "droonga/process_supervisor"

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

        setup_path
        setup_log

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
        parser.version = Engine::VERSION
        @configuration.add_command_line_options(parser)
        parser.parse!(command_line_arguments)
      end

      def setup_path
        Path.setup
        unless $0 == File.basename($0)
          droonga_engine_bin_path = File.expand_path(File.dirname($0))
          new_paths = [
            droonga_engine_bin_path,
            ENV["PATH"],
          ]
          ENV["PATH"] = new_paths.join(File::PATH_SEPARATOR)
        end
      end

      def setup_log
        ENV["DROONGA_LOG_LEVEL"] = @configuration.log_level
      end

      def run_main_loop
        main_loop = MainLoop.new(@configuration)
        main_loop.run
      end

      def open_log_file
        if @configuration.log_file_path
          @configuration.log_file_path.open("a") do |file|
            $stdout.reopen(file)
            $stderr.reopen(file)
            yield
          end
        else
          yield
        end
      end

      def write_pid_file
        if @configuration.pid_file_path
          @configuration.pid_file_path.open("w") do |file|
            file.puts(Process.pid)
          end
          begin
            yield
          ensure
            FileUtils.rm_f(@configuration.pid_file_path.to_s)
          end
        else
          yield
        end
      end

      class Configuration
        attr_reader :ready_notify_fd
        def initialize
          @config = nil

          @host = nil
          @port = nil
          @tag  = nil

          @log_level       = nil
          @log_file        = nil
          @daemon          = nil
          @pid_file_path   = nil
          @ready_notify_fd = nil
        end

        def engine_name
          "#{host}:#{port}/#{tag}"
        end

        def address_family
          ip_address = IPAddr.new(IPSocket.getaddress(host))
          ip_address.family
        end

        def host
          @host || config["host"] || default_host
        end

        def port
          @port || config["port"] || default_port
        end

        def tag
          @tag || config["tag"] || default_tag
        end

        def log_level
          @log_level || config["log_level"] || default_log_level
        end

        def log_file_path
          @log_file_path || config["log_file"] || default_log_file_path
        end

        def pid_file_path
          @pid_file_path || config["pid_file"] || default_pid_file_path
        end

        def daemon?
          daemon = @daemon
          daemon = config["daemon"] if daemon.nil?
          daemon = false if daemon.nil?
          daemon
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
          add_notification_options(parser)
        end

        def listen_socket
          @listen_socket ||= TCPServer.new(host, port)
        end

        def heartbeat_socket
          @heartbeat_socket ||= bind_heartbeat_socket
        end

        private
        def default_host
          Address::DEFAULT_HOST
        end

        def default_port
          Address::DEFAULT_PORT
        end

        def default_tag
          Address::DEFAULT_TAG
        end

        def default_log_level
          ENV["DROONGA_LOG_LEVEL"] || Logger::Level.default
        end

        def default_log_file_path
          nil
        end

        def default_pid_file_path
          nil
        end

        def normalize_path(path)
          if path == "-"
            nil
          else
            Pathname.new(path).expand_path
          end
        end

        def config
          @config ||= load_config
        end

        def load_config
          config_path = Path.config
          return {} unless config_path.exist?

          config = YAML.load_file(config_path)
          path_keys = ["log_file", "pid_file"]
          path_keys.each do |path_key|
            path = config[path_key]
            next if path.nil?

            path = Pathname.new(path)
            unless path.absolute?
              path = (config_path.dirname + path).expand_path
            end
            config[path_key] = path
          end
          config
        end

        def add_connection_options(parser)
          parser.separator("")
          parser.separator("Connection:")
          parser.on("--host=HOST",
                    "The host name of the Droonga engine",
                    "(#{default_host})") do |host|
            @host = host
          end
          parser.on("--port=PORT", Integer,
                    "The port number of the Droonga engine",
                    "(#{default_port})") do |port|
            @port = port
          end
          parser.on("--tag=TAG",
                    "The tag of the Droonga engine",
                    "(#{default_tag})") do |tag|
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
                    "(#{default_log_level})") do |level|
            @log_level = level
          end
          parser.on("--log-file=FILE",
                    "Output logs to FILE",
                    "(#{default_log_file_path})") do |path|
            @log_file_path = normalize_path(path)
          end
        end

        def add_process_options(parser)
          parser.separator("")
          parser.separator("Process:")
          parser.on("--daemon",
                    "Run as a daemon") do
            @daemon = true
          end
          parser.on("--no-daemon",
                    "Run as a regular process") do
            @daemon = false
          end
          parser.on("--pid-file=PATH",
                    "Put PID to PATH") do |path|
            @pid_file_path = normalize_path(path)
          end
        end

        def add_path_options(parser)
          parser.separator("")
          parser.separator("Path:")
          parser.on("--base-dir=DIR",
                    "Use DIR as the base directory",
                    "(#{Path.base})") do |dir|
            Path.base = File.expand_path(dir)
            @config = nil
          end
        end

        def add_notification_options(parser)
          parser.separator("")
          parser.separator("Notification:")
          parser.on("--ready-notify-fd=FD", Integer,
                    "Send 'ready' message to FD on ready") do |fd|
            @ready_notify_fd = fd
          end
        end

        def bind_heartbeat_socket
          socket = UDPSocket.new(address_family)
          socket.bind(host, port)
          socket
        end
      end

      class MainLoop
        def initialize(configuration)
          @configuration = configuration
          @loop = Coolio::Loop.default
        end

        def run
          start_serf
          @service_runner = run_service
          setup_initial_on_ready
          @catalog_observer = run_catalog_observer
          @command_runner = run_command_runner

          trap_signals
          @loop.run

          @service_runner.success?
        end

        private
        def setup_initial_on_ready
          return if @configuration.ready_notify_fd.nil?
          @service_runner.on_ready = lambda do
            output = IO.new(@configuration.ready_notify_fd)
            output.puts("ready")
            output.close
          end
        end

        def trap_signals
          trap(:TERM) do
            @command_runner.push_command(:stop_gracefully)
            trap(:TERM, "DEFAULT")
          end
          trap(:INT) do
            @command_runner.push_command(:stop_immediately)
            trap(:INT, "DEFAULT")
          end
          trap(:QUIT) do
            @command_runner.push_cmmand(:stop_immediately)
            trap(:QUIT, "DEFAULT")
          end
          trap(:USR1) do
            @command_runner.push_command(:restart_graceful)
          end
          trap(:HUP) do
            @command_runner.push_command(:restart_immediately)
          end
          trap(:USR2) do
            Sigdump.dump
          end
        end

        def stop_gracefully
          @command_runner.stop
          @catalog_observer.stop
          @serf.leave
          @serf_agent.stop
          @service_runner.stop_gracefully
        end

        def stop_immediately
          @command_runner.stop
          @catalog_observer.stop
          @serf.leave
          @serf_agent.stop
          @service_runner.stop_immediately
        end

        def restart_graceful
          old_service_runner = @service_runner
          @service_runner = run_service
          @service_runner.on_ready = lambda do
            @service_runner.on_failure = nil
            old_service_runner.stop_gracefully
          end
          @service_runner.on_failure = lambda do
            @service_runner.on_failure = nil
            @service_runner = old_service_runner
          end
        end

        def restart_immediately
          old_service_runner = @service_runner
          @service_runner = run_service
          old_service_runner.stop_immediately
        end

        def run_service
          service_runner = ServiceRunner.new(@loop, @configuration)
          service_runner.run
          service_runner
        end

        def start_serf
          @serf = Serf.new(@configuration.engine_name)
          @serf_agent = @serf.run_agent(@loop)
        end

        def run_catalog_observer
          catalog_observer = FileObserver.new(@loop, Path.catalog)
          catalog_observer.on_change = lambda do
            restart_graceful
            @serf.update_cluster_id
          end
          catalog_observer.start
          catalog_observer
        end

        def run_command_runner
          command_runner = CommandRunner.new(@loop)
          command_runner.on_command = lambda do |command|
            __send__(command)
          end
          command_runner.start
          command_runner
        end
      end

      class ServiceRunner
        def initialize(raw_loop, configuration)
          @raw_loop = raw_loop
          @configuration = configuration
          @success = false
          @on_ready = nil
          @on_failure = nil
        end

        def on_ready=(callback)
          @on_ready = callback
        end

        def on_failure=(callback)
          @on_failure = callback
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
            "droonga-engine-service",
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
        def create_process_supervisor(input, output)
          supervisor = ProcessSupervisor.new(@raw_loop, input, output)
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
          @on_failure.call if @on_failure
        end

        def on_finish
          _, status = Process.waitpid2(@pid)
          @success = status.success?
          @supervisor.stop
          on_failure unless success?
        end
      end

      class CommandRunner
        attr_writer :on_command
        def initialize(loop)
          @loop = loop
          @commands = []
          @on_command = nil
        end

        def start
          @async_watcher = Coolio::AsyncWatcher.new
          on_signal = lambda do
            commands = @commands.uniq
            @commands.clear
            until commands.empty?
              command = commands.shift
              @on_command.call(command) if @on_command
            end
          end
          @async_watcher.on_signal do
            on_signal.call
          end
          @loop.attach(@async_watcher)
        end

        def stop
          return if @async_watcher.nil?
          @async_watcher.detach
          @async_watcher = nil
        end

        def push_command(command)
          return if @async_watcher.nil?
          first_command_p = @commands.empty?
          @commands << command
          @async_watcher.signal if first_command_p
        end
      end
    end
  end
end
