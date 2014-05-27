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
require "pathname"

require "droonga/path"
require "droonga/engine"
require "droonga/serf"
require "droonga/event_loop"
require "droonga/fluent_message_receiver"
require "droonga/internal_fluent_message_receiver"
require "droonga/plugin_loader"

module Droonga
  module Command
    module DroongaEngine
      module Signals
        include ServerEngine::Daemon::Signals
      end

      class SupervisorConfiguration
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
                    "(#{Droonga::Path.base})") do |dir|
            Droonga::Path.base = dir
          end
        end

        def bind_heartbeat_socket
          socket = UDPSocket.new(address_family)
          socket.bind(@host, @port)
          socket
        end
      end

      class Supervisor
        class << self
          def run(command_line_arguments)
            new.run(command_line_arguments)
          end
        end

        def initialize
          @configuration = SupervisorConfiguration.new
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
          Droonga::Path.base
        end

        def run_service(loop)
          service_runner = ServiceRunner.new(loop, @configuration)
          service_runner.run
          service_runner
        end

        def run_serf(loop)
          serf = Serf.new(loop, @configuration.engine_name)
          serf.start
          serf
        end

        def run_main_loop
          raw_loop = Coolio::Loop.default

          serf = nil
          service_runner = nil
          trap(:INT) do
            serf.shutdown if serf
            service_runner.stop_immediately if service_runner
          end
          trap(Signals::GRACEFUL_STOP) do
            serf.shutdown if serf
            service_runner.stop_graceful if service_runner
          end
          trap(Signals::IMMEDIATE_STOP) do
            serf.shutdown if serf
            service_runner.stop_immediately if service_runner
          end
          trap(Signals::GRACEFUL_RESTART) do
            old_service_runner = service_runner
            service_runner = run_service(raw_loop)
            service_runner.on_ready = lambda do
              old_service_runner.stop_graceful
            end
          end
          trap(Signals::IMMEDIATE_RESTART) do
            old_service_runner = service_runner
            service_runner = run_service(raw_loop)
            old_service_runner.stop_immediately
          end

          serf = run_serf(raw_loop)
          service_runner = run_service(raw_loop)
          raw_loop.run
          serf.shutdown if serf.running?

          service_runner.success?
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
      end

      class ServiceRunner
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

        def stop_graceful
          @control_write_out.write("stop-graceful\n")
        end

        def stop_immediately
          @control_write_out.write("stop-immediately\n")
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
              when "ready\n"
                on_ready
              when "finish\n"
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

      class Service
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
          begin
            run_services
          rescue
            logger.exception("failed to run services", $!)
          ensure
            unless @control_write_closed
              control_write_io.write("finish\n")
              control_write_io.close
            end
          end

          true
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
          run_engine
          run_receiver
          run_control_io
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
          return if @internal_message_receiver.nil?
          @internal_message_receiver, receiver = nil, @internal_message_receiver
          receiver.shutdown
        end

        def run_engine
          @engine = Engine.new(@loop, @engine_name, @internal_engine_name)
          @engine.start
        end

        def run_receiver
          @receiver = create_receiver
          @receiver.start
        end

        def shutdown_receiver
          return if @receiver.nil?
          @receiver, receiver = nil, @receiver
          receiver.shutdown
        end

        def run_control_io
          @control_read = Coolio::IO.new(IO.new(@control_read_fd))
          @control_read_fd = nil
          on_read = lambda do |data|
            # TODO: should buffer data to handle half line received case
            data.each_line do |line|
              case line
              when "stop-graceful\n"
                stop_graceful
              when "stop-immediately\n"
                stop_immediately
              end
            end
          end
          @control_read.on_read do |data|
            on_read.call(data)
          end
          read_on_close = lambda do
            if @control_read
              @control_read = nil
              stop_immediately
            end
          end
          @control_read.on_close do
            read_on_close.call
          end
          @loop.attach(@control_read)

          @control_write = Coolio::IO.new(IO.new(@control_write_fd))
          @control_write_fd = nil
          write_on_close = lambda do
            if @control_write
              @control_write = nil
              stop_immediately
            end
            @control_write_closed = true
          end
          @control_write.on_close do
            write_on_close.call
          end
          @loop.attach(@control_write)

          @control_write.write("ready\n")
        end

        def shutdown_control_io
          if @control_write
            @control_write, control_write = nil, @control_write
            control_write.detach
          end
          if @control_read
            @control_read, control_read = nil, @control_read
            control_read.close
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

        def stop_graceful
          return if @stopping
          @stopping = true
          shutdown_receiver
          @engine.stop_graceful do
            shutdown_control_io
            shutdown_internal_message_receiver
          end
        end

        # It may be called after stop_graceful.
        def stop_immediately
          shutdown_control_io
          shutdown_receiver if @receiver
          shutdown_internal_message_receiver
          @engine.stop_immediately
          @loop.stop
        end

        def log_tag
          "service"
        end
      end
    end
  end
end
