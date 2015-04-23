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
require "droonga/loggable"
require "droonga/deferrable"
require "droonga/path"
require "droonga/node_name"
require "droonga/forwarder"
require "droonga/serf"
require "droonga/cluster"
require "droonga/file_observer"
require "droonga/process_supervisor"
require "droonga/differ"

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

        run_main_loop
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

      class Configuration
        attr_reader :ready_notify_fd
        def initialize
          @config = nil

          @host = nil
          @port = nil
          @tag  = nil

          @internal_connection_lifetime = nil

          @log_level       = nil
          @log_file        = nil
          @daemon          = nil
          @pid_file_path   = nil
          @ready_notify_fd = nil

          @listen_fd       = nil
          @heartbeat_fd    = nil
          @serf_agent_pid  = nil
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

        def internal_connection_lifetime
          @internal_connection_lifetime ||
            config["internal_connection_lifetime"] ||
            default_internal_connection_lifetime
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

        def to_engine_command_line
          command_line_options = [
            "--host", host,
            "--port", port.to_s,
            "--tag", tag,
            "--internal-connection-lifetime",
              internal_connection_lifetime.to_s,
            "--log-level", log_level,
          ]
          if log_file_path
            command_line_options.concat(["--log-file", log_file_path.to_s])
          end
          if pid_file_path
            command_line_options.concat(["--pid-file", pid_file_path.to_s])
          end
          if daemon?
            command_line_options << "--daemon"
          else
            command_line_options << "--no-daemon"
          end
          command_line_options
        end

        def to_service_command_line
          command_line_options = [
            "--engine-name", engine_name,
            "--internal-connection-lifetime",
              internal_connection_lifetime.to_s,
          ]
          command_line_options
        end

        def add_command_line_options(parser)
          add_connection_options(parser)
          add_log_options(parser)
          add_process_options(parser)
          add_path_options(parser)
          add_notification_options(parser)
          add_internal_options(parser)
        end

        def listen_socket
          @listen_socket ||= create_listen_socket
        end

        def heartbeat_socket
          @heartbeat_socket ||= create_heartbeat_socket
        end

        def serf_agent_pid
          @serf_agent_pid
        end

        private
        def default_host
          NodeName::DEFAULT_HOST
        end

        def default_port
          NodeName::DEFAULT_PORT
        end

        def default_tag
          NodeName::DEFAULT_TAG
        end

        def default_internal_connection_lifetime
          Forwarder::DEFAULT_AUTO_CLOSE_TIMEOUT_SECONDS
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
          parser.on("--internal-connection-lifetime=SECONDS", Float,
                    "The time to expire internal connections, in seconds",
                    "(#{default_internal_connection_lifetime})") do |seconds|
            @internal_connection_lifetime = seconds
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

        def add_internal_options(parser)
          parser.separator("")
          parser.separator("Internal:")
          parser.on("--listen-fd=FD", Integer,
                    "FD of listen socket") do |fd|
            @listen_fd = fd
          end
          parser.on("--heartbeat-fd=FD", Integer,
                    "FD of heartbeat socket") do |fd|
            @heartbeat_fd = fd
          end
          parser.on("--serf-agent-pid=PID", Integer,
                    "PID of Serf agent") do |pid|
            @serf_agent_pid = pid
          end
        end

        def create_listen_socket
          begin
            TCPServer.new(host, port)
          rescue Errno::EADDRINUSE
            raise if @listen_fd.nil?
            TCPServer.for_fd(@listen_fd)
          end
        end

        def create_heartbeat_socket
          begin
            socket = UDPSocket.new(address_family)
            socket.bind(host, port)
            socket
          rescue Errno::EADDRINUSE
            raise if @heartbeat_fd.nil?
            UDPSocket.for_fd(@heartbeat_fd)
          end
        end
      end

      class MainLoop
        include Loggable

        def initialize(configuration)
          @configuration = configuration
          ENV["DROONGA_ENGINE_NAME"] = @configuration.engine_name
          @loop = Coolio::Loop.default
          @log_file = nil
          @pid_file_path = nil
        end

        def run
          reopen_log_file
          write_pid_file do
            run_internal
          end
        end

        private
        def reopen_log_file
          return if @configuration.log_file_path.nil?
          @log_file = @configuration.log_file_path.open("a")
          $stdout.reopen(@log_file)
          $stderr.reopen(@log_file)
        end

        def write_pid_file
          @pid_file_path = @configuration.pid_file_path
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

        def run_internal
          logger.trace("run_internal: start")
          start_serf
          @serf_agent.on_ready = lambda do
            logger.trace("run_internal: serf agent is ready")
            @serf.initialize_tags
            @serf.update_cluster_state
            @service_runner = run_service
            setup_initial_on_ready
            @catalog_observer = run_catalog_observer
            @cluster_state_observer = run_cluster_state_observer
            @command_runner = run_command_runner
          end

          trap_signals
          @loop.run

          while @service_runner.nil? do
            sleep 1
          end

          succeeded = @service_runner.success?
          logger.trace("run_internal: done")
          succeeded
        end

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
          logger.trace("stop_gracefully: start")
          logger.trace("stop_gracefully: stopping serf agent")
          stop_serf do
            logger.trace("stop_gracefully: stopping command runner")
            @command_runner.stop
            logger.trace("stop_gracefully: stopping cluster_state_observer")
            @cluster_state_observer.stop
            logger.trace("stop_gracefully: stopping catalog_observer")
            @catalog_observer.stop
            @service_runner.stop_gracefully
            logger.trace("stop_gracefully: completely done")
          end
          logger.trace("stop_gracefully: done")
        end

        def stop_immediately
          stop_serf do
            @command_runner.stop
            @cluster_state_observer.stop
            @catalog_observer.stop
            @service_runner.stop_immediately
          end
        end

        def restart_graceful
          return if @restarting
          @restarting = true
          logger.trace("restart_graceful: start")
          old_service_runner = @service_runner
          reopen_log_file
          @service_runner = run_service
          @service_runner.on_ready = lambda do
            logger.info("restart_graceful: new service runner is ready")
            @service_runner.on_failure = nil
            @service_runner.refresh_self_reference
            old_service_runner.stop_gracefully
            @restarting = false
            logger.trace("restart_graceful: done")
          end
          @service_runner.on_failure = lambda do
            logger.info("restart_graceful: failed to setup new service runner")
            @service_runner.on_failure = nil
            @service_runner = old_service_runner
            @restarting = false
            logger.trace("restart_graceful: failed")
          end
        end

        def restart_immediately
          return if @restarting
          @restarting = true
          old_service_runner = @service_runner
          reopen_log_file
          @service_runner = run_service
          old_service_runner.stop_immediately
          @restarting = false
        end

        def restart_self
          logger.trace("restart_self: start")
          old_pid_file_path = Pathname.new("#{@pid_file_path}.old")
          FileUtils.mv(@pid_file_path.to_s, old_pid_file_path.to_s)
          @pid_file_path = old_pid_file_path
          stop_gracefully

          engine_runner = EngineRunner.new(@configuration)
          engine_runner.run
          logger.trace("restart_self: done")
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

        def stop_serf(&block)
          logger.trace("stop_serf: start")
          begin
            @serf.leave
          rescue Droonga::Serf::Command::Failure
            logger.error("Failed to leave from Serf cluster: #{$!.message}")
          end
          @serf_agent.stop do
            logger.trace("stop_serf: serf agent stopped")
            yield
          end
          logger.trace("stop_serf: done")
        end

        def run_catalog_observer
          catalog_observer = FileObserver.new(@loop, Path.catalog)
          catalog_observer.on_change = lambda do
            logger.info("restart by updated catalog.json")
            restart_graceful
            @serf.update_cluster_id
          end
          catalog_observer.start
          catalog_observer
        end

        RESTART_TRIGGER_KEYS = [
          "role",
          "accept_messages_newer_than",
        ]

        def run_cluster_state_observer
          previous_state = nil
          cluster_state_observer = FileObserver.new(@loop, Path.cluster_state)
          cluster_state_observer.on_change = lambda do
            my_name   = @configuration.engine_name
            new_state = Cluster.load_state_file
            if new_state and previous_state
              my_new_state = new_state[my_name].select do |key, _value|
                RESTART_TRIGGER_KEYS.include?(key)
              end
              my_previous_state = previous_state[my_name].select do |key, _value|
                RESTART_TRIGGER_KEYS.include?(key)
              end
              if my_new_state != my_previous_state
                logger.info("restart by changes of myself in cluster-state.json",
                            :previous => my_previous_state,
                            :new      => my_new_state,
                            :diff     => Differ.diff(my_previous_state, my_new_state))
                restart_graceful
              end
            end
            previous_state = new_state
          end
          cluster_state_observer.start
          cluster_state_observer
        end

        def run_command_runner
          command_runner = CommandRunner.new(@loop)
          command_runner.on_command = lambda do |command|
            __send__(command)
          end
          command_runner.start
          command_runner
        end

        def log_tag
          "droonga-engine"
        end
      end

      class EngineRunner
        def initialize(configuration)
          @configuration = configuration
        end

        def run
          listen_fd = @configuration.listen_socket.fileno
          heartbeat_fd = @configuration.heartbeat_socket.fileno
          env = {}
          command_line = [
            RbConfig.ruby,
            "-S",
            "droonga-engine",
            "--listen-fd", listen_fd.to_s,
            "--heartbeat-fd", heartbeat_fd.to_s,
            *@configuration.to_engine_command_line,
          ]
          options = {
            listen_fd => listen_fd,
            heartbeat_fd => heartbeat_fd,
          }
          spawn(env, *command_line, options)
        end
      end

      class ServiceRunner
        include Loggable
        include Deferrable

        def initialize(raw_loop, configuration)
          @raw_loop = raw_loop
          @configuration = configuration
          @success = false
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
            *@configuration.to_service_command_line,
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
          logger.trace("stop_gracefully: start")
          @supervisor.stop_gracefully
          logger.trace("stop_gracefully: done")
        end

        def stop_immediately
          logger.trace("stop_immediately: start")
          @supervisor.stop_immediately
          logger.trace("stop_immediately: done")
        end

        def success?
          @success
        end

        def refresh_self_reference
          @supervisor.refresh_self_reference
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

        def on_finish
          _, status = Process.waitpid2(@pid)
          @success = status.success?
          @supervisor.stop
          on_failure unless success?
        end

        def log_tag
          "service_runner"
        end
      end

      class CommandRunner
        include Loggable

        attr_writer :on_command
        def initialize(loop)
          @loop = loop
          @commands = []
          @on_command = nil
        end

        def start
          logger.trace("start: stert")
          @async_watcher = Coolio::AsyncWatcher.new
          @async_watcher.on_signal do
            commands = @commands.uniq
            @commands.clear
            until commands.empty?
              command = commands.shift
              @on_command.call(command) if @on_command
            end
          end
          @loop.attach(@async_watcher)
          logger.trace("start: async watcher attached",
                       :watcher => @async_watcher)
          logger.trace("start: done")
        end

        def stop
          return if @async_watcher.nil?
          logger.trace("stop: stert")
          @async_watcher.detach
          # logger.trace("stop: watcher detached", :watcher => @async_watcher)
          @async_watcher = nil
          logger.trace("stop: done")
        end

        def push_command(command)
          return if @async_watcher.nil?
          first_command_p = @commands.empty?
          @commands << command
          @async_watcher.signal if first_command_p
        end

        def log_tag
          "command_runner"
        end
      end
    end
  end
end
