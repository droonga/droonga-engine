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

require "droonga/engine"
require "droonga/event_loop"
require "droonga/fluent_message_receiver"
require "droonga/plugin_loader"

module Droonga
  class Engine
    module Command
      module DroongaEngine
        module Signals
          include ServerEngine::Daemon::Signals
        end

        class Configuration
          DEFAULT_HOST = Socket.gethostname
          DEFAULT_PORT = 10031

          attr_reader :host, :port, :tag, :live_nodes_file, :log_file, :pid_file
          def initialize
            @host = DEFAULT_HOST
            @port = DEFAULT_PORT
            @tag = "droonga"
            @live_nodes_file = nil
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
              "--host", @host,
              "--port", @port.to_s,
              "--tag", @tag,
              "--log-level", log_level,
            ]
            if live_nodes_file
              command_line_options += [
                "--live-nodes-file", live_nodes_file.to_s,
              ]
            end
            command_line_options
          end

          def add_command_line_options(parser)
            add_connection_options(parser)
            add_log_options(parser)
            add_process_options(parser)
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
            parser.on("--live-nodes-file=FILE",
                      "Path to the list file of live nodes") do |file|
              @live_nodes_file = Pathname(file)
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
        end

        class Supervisor
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

            @listen_socket = TCPServer.new(@configuration.host,
                                           @configuration.port)
            @heartbeat_socket = UDPSocket.new(@configuration.address_family)
            @heartbeat_socket.bind(@configuration.host,
                                   @configuration.port)

            if @configuration.daemon?
              ENV["DROONGA_CATALOG"] ||= "catalog.json"
              ENV["DROONGA_CATALOG"] = File.expand_path(ENV["DROONGA_CATALOG"])
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

          def run_service(ready_notify_fd=nil)
            listen_fd = @listen_socket.fileno
            heartbeat_fd = @heartbeat_socket.fileno
            env = {}
            command_line = [
              RbConfig.ruby,
              "-S",
              "#{$0}-service",
              "--listen-fd", listen_fd.to_s,
              "--heartbeat-fd", heartbeat_fd.to_s,
              *@configuration.to_command_line
            ]
            options = {
              listen_fd => listen_fd,
              heartbeat_fd => heartbeat_fd,
            }
            if ready_notify_fd
              command_line.push("--ready-notify-fd", ready_notify_fd.to_s)
              options[ready_notify_fd] = ready_notify_fd
            end
            if @log_output
              options[:out] = @log_output
              options[:err] = @log_output
            end
            spawn(env, *command_line, options)
          end

          def run_main_loop
            service_pid = nil
            running = true

            trap(:INT) do
              Process.kill(:INT, service_pid)
              running = false
            end
            trap(Signals::GRACEFUL_STOP) do
              Process.kill(Signals::GRACEFUL_STOP, service_pid)
              running = false
            end
            trap(Signals::IMMEDIATE_STOP) do
              Process.kill(Signals::IMMEDIATE_STOP, service_pid)
              running = false
            end
            trap(Signals::GRACEFUL_RESTART) do
              old_service_pid = service_pid
              IO.pipe do |ready_notify_read_io, ready_notify_write_io|
                service_pid = run_service(ready_notify_write_io.fileno)
                ready_notify_write_io.close
                IO.select([ready_notify_read_io])
                Process.kill(Signals::GRACEFUL_STOP, old_service_pid)
              end
            end
            trap(Signals::IMMEDIATE_RESTART) do
              old_service_pid = service_pid
              service_pid = run_service
              Process.kill(Signals::IMMEDIATE_STOP, old_service_pid)
            end

            succeeded = true
            while running
              service_pid ||= run_service
              finished_pid, status = Process.waitpid2(service_pid)
              service_pid = nil if service_pid == finished_pid
              if status.nil?
                succeeded = false
                break
              end
              unless status.success?
                succeeded = false
                break
              end
            end

            succeeded
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

        class Service
          class << self
            def run(command_line_arguments)
              new.run(command_line_arguments)
            end
          end

          def initialize
            @configuration = Configuration.new
            @listen_fd = nil
            @heartbeat_fd = nil
            @ready_notiofy_fd = nil
          end

          def run(command_line_arguments)
            parse_command_line_arguments!(command_line_arguments)
            PluginLoader.load_all

            begin
              run_services
            ensure
              shutdown_services
            end

            true
          end

          private
          def parse_command_line_arguments!(command_line_arguments)
            parser = OptionParser.new
            @configuration.add_command_line_options(parser)
            add_internal_options(parser)
            parser.parse!(command_line_arguments)
          end

          def add_internal_options(parser)
            parser.separator("")
            parser.separator("Internal:")
            parser.on("--listen-fd=FD", Integer,
                      "Use FD as the listen file descriptor") do |fd|
              @listen_fd = fd
            end
            parser.on("--heartbeat-fd=FD", Integer,
                      "Use FD as the heartbeat file descriptor") do |fd|
              @heartbeat_fd = fd
            end
            parser.on("--ready-notify-fd=FD", Integer,
                      "Use FD for notifying the service ready") do |fd|
              @ready_notify_fd = fd
            end
          end

          def run_services
            @engine = nil
            @receiver = nil
            raw_loop = Coolio::Loop.default
            @loop = EventLoop.new(raw_loop)

            run_engine
            run_receiver
            setup_signals
            notify_ready
            @loop.run
          end

          def shutdown_services
            shutdown_receiver
            shutdown_engine
            @loop = nil
          end

          def run_engine
            engine_options = {
              :live_nodes_file => @configuration.live_nodes_file,
            }
            @engine = Engine.new(@loop, @configuration.engine_name, engine_options)
            @engine.start
          end

          def shutdown_engine
            return if @engine.nil?
            @engine.shutdown
            @engine = nil
          end

          def run_receiver
            @receiver = create_receiver
            @receiver.start
          end

          def shutdown_receiver
            return if @receiver.nil?
            @receiver.shutdown
            @receiver = nil
          end

          def create_receiver
            options = {
              :host => @host,
              :port => @port,
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

          def setup_signals
            trap(Signals::GRACEFUL_STOP) do
              stop_graceful
            end
            trap(Signals::IMMEDIATE_STOP) do
              stop_immediate
            end
            trap(:INT) do
              stop_immediate
              trap(:INT, "DEFAULT")
            end
          end

          def stop_graceful
            @loop.stop if @loop
          end

          def stop_immediate
            stop_graceful
            shutdown_services
          end

          def notify_ready
            return if @ready_notify_fd.nil?
            ready_notify_io = IO.new(@ready_notify_fd)
            @ready_notify_fd = nil
            watcher = Coolio::IOWatcher.new(ready_notify_io, "w")
            @loop.attach(watcher)
            watcher.on_writable do
              ready_notify_io.write("ready\n")
              ready_notify_io.close
              detach
            end
          end
        end
      end
    end
  end
end
