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

require "droonga/engine"
require "droonga/event_loop"
require "droonga/fluent_message_receiver"
require "droonga/plugin_loader"

module Droonga
  class Engine
    module Command
      module DroongaEngine
        class Configuration
          DEFAULT_HOST = Socket.gethostname
          DEFAULT_PORT = 10031

          attr_reader :host, :port, :tag
          def initialize
            @host = DEFAULT_HOST
            @port = DEFAULT_PORT
            @tag = "droonga"
          end

          def engine_name
            "#{@host}:#{@port}/#{@tag}"
          end

          def address_family
            ip_address = IPAddr.new(IPSocket.getaddress(@host))
            ip_address.family
          end

          def log_level
            ENV["DROOGNA_LOG_LEVEL"] || Logger::Level.default_label
          end

          def to_command_line
            [
              "--host", @host,
              "--port", @port.to_s,
              "--tag", @tag,
              "--log-level", log_level,
            ]
          end

          def add_command_line_options(parser)
            add_connection_options(parser)
            add_log_options(parser)
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
              ENV["DROOGNA_LOG_LEVEL"] = level
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
          end

          def run(command_line_arguments)
            parse_command_line_arguments!(command_line_arguments)

            @listen_socket = TCPServer.new(@configuration.host,
                                           @configuration.port)
            @heartbeat_socket = UDPSocket.new(@configuration.address_family)
            @heartbeat_socket.bind(@configuration.host,
                                   @configuration.port)

            run_main_loop
          end

          private
          def parse_command_line_arguments!(command_line_arguments)
            parser = OptionParser.new
            @configuration.add_command_line_options(parser)
            parser.parse!(command_line_arguments)
          end

          def run_service
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
            spawn(env, *command_line, options)
          end

          def run_main_loop
            service_pid = nil
            running = true

            trap(:INT) do
              Process.kill(:INT, service_pid)
              running = false
            end
            trap(ServerEngine::Daemon::Signals::GRACEFUL_STOP) do
              Process.kill(ServerEngine::Daemon::Signals::GRACEFUL_STOP,
                           service_pid)
              running = false
            end
            trap(ServerEngine::Daemon::Signals::IMMEDIATE_STOP) do
              Process.kill(ServerEngine::Daemon::Signals::IMMEDIATE_STOP,
                           service_pid)
              running = false
            end

            while running
              service_pid = run_service
              _, status = Process.waitpid2(service_pid)
              break if status.nil?
              break unless status.success?
            end

            true
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
          end

          def run_services
            @engine = nil
            @receiver = nil
            raw_loop = Coolio::Loop.default
            @loop = EventLoop.new(raw_loop)

            run_engine
            run_receiver
            setup_signals
            @loop.run
          end

          def shutdown_services
            shutdown_receiver
            shutdown_engine
            @loop = nil
          end

          def run_engine
            @engine = Engine.new(@loop, @configuration.engine_name)
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
            trap(ServerEngine::Daemon::Signals::GRACEFUL_STOP) do
              stop_graceful
            end
            trap(ServerEngine::Daemon::Signals::IMMEDIATE_STOP) do
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
        end
      end
    end
  end
end
