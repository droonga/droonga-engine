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

require "coolio"

require "droonga/engine"
require "droonga/fluent_message_receiver"
require "droonga/internal_fluent_message_receiver"
require "droonga/plugin_loader"

module Droonga
  module Command
    class DroongaEngineService
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
        "droonga-engine-service"
      end
    end
  end
end
