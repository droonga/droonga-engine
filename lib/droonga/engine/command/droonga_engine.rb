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

require "droonga/engine"
require "droonga/event_loop"
require "droonga/fluent_message_receiver"
require "droonga/plugin_loader"

module Droonga
  class Engine
    module Command
      class DroongaEngine
        class << self
          def run(command_line_arguments)
            new.run(command_line_arguments)
          end
        end

        def initialize
          @host = FluentMessageReceiver::DEFAULT_HOST
          @port = FluentMessageReceiver::DEFAULT_PORT
          @tag = "droonga"
          @log_level = Logger::Level.default_label
        end

        def run(command_line_arguments)
          parse_command_line_arguments!(command_line_arguments)
          PluginLoader.load_all

          raw_loop = Coolio::Loop.default
          loop = EventLoop.new(raw_loop)

          run_engine(loop) do |engine|
            run_receiver(loop, engine) do |receiver|
              trap(:INT) do
                loop.stop
              end
              trap(:TERM) do
                loop.stop
              end
              loop.run
            end
          end

          true
        end

        private
        def parse_command_line_arguments!(command_line_arguments)
          parser = OptionParser.new
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
          levels = Logger::Level::LABELS
          levels_label = levels.join(",")
          parser.on("--log-level=LEVEL", levels,
                    "The log level of the Droonga engine",
                    "[#{levels_label}]",
                    "(#{@log_level})") do |level|
            @log_level = level
          end
          parser.parse!(command_line_arguments)

          ENV["DROOGNA_LOG_LEVEL"] = @log_level
        end

        def run_engine(loop)
          engine = Engine.new(loop, engine_name)
          begin
            engine.start
            yield(engine)
          ensure
            engine.shutdown
          end
        end

        def engine_name
          "#{@host}:#{@port}/#{@tag}"
        end

        def run_receiver(loop, engine)
          receiver_options = {
            :host => options.host,
            :port => options.port,
          }
          on_message = lambda do |tag, time, record|
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
            message

            engine.process(message)
          end
          receiver = FluentMessageReceiver.new(loop,
                                               receiver_options,
                                               &on_message)
          begin
            receiver.start
            yield(receiver)
          ensure
            receiver.shutdown
          end
        end
      end
    end
  end
end
