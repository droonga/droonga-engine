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

require "socket"

require "msgpack"

require "droonga/loggable"
require "droonga/event_loop"

module Droonga
  class FluentMessageReceiver
    include Loggable

    def initialize(loop, options={}, &on_message)
      @loop = loop
      @listen_fd = options[:listen_fd]
      @heartbeat_fd = options[:heartbeat_fd]
      @server = nil
      @clients = []
      @on_message = on_message
    end

    def start
      logger.trace("start: start")
      start_heartbeat_receiver
      start_server
      logger.trace("start: done")
    end

    def stop_gracefully
      logger.trace("stop_gracefully: start")
      shutdown_heartbeat_receiver
      logger.trace("stop_gracefully: middle")
      shutdown_server
      logger.trace("stop_gracefully: done")
    end

    def stop_immediately
      logger.trace("stop_immediately: start")
      stop_gracefully
      shutdown_clients
      logger.trace("stop_immediately: done")
    end

    def shutdown_clients
      @clients.dup.each do |client|
        client.close
      end
    end

    private
    def start_heartbeat_receiver
      logger.trace("start_heartbeat_receiver: start")
      @heartbeat_receiver = HeartbeatReceiver.new(@loop, @heartbeat_fd)
      @heartbeat_receiver.start
      logger.trace("start_heartbeat_receiver: done")
    end

    def shutdown_heartbeat_receiver
      logger.trace("shutdown_heartbeat_receiver: start")
      @heartbeat_receiver.shutdown
      logger.trace("shutdown_heartbeat_receiver: done")
    end

    def start_server
      logger.trace("start_server: start")

      @clients = []
      @server = create_server do |connection|
        client = Client.new(connection) do |tag, time, record|
          @on_message.call(tag, time, record)
        end
        client.on_close = lambda do
          @clients.delete(client)
        end
        @clients << client
      end
      @loop.attach(@server)

      logger.trace("start_server: done")
    end

    def create_server(&block)
      Coolio::Server.new(TCPServer.for_fd(@listen_fd), Coolio::TCPSocket, &block)
    end

    def shutdown_server
      logger.trace("shutdown_server: start")
      @server.close
      logger.trace("shutdown_server: done")
    end

    def log_tag
      "fluent-message-receiver"
    end

    class HeartbeatReceiver
      def initialize(loop, fd)
        @loop = loop
        @fd = fd
      end

      def start
        @socket = UDPSocket.for_fd(@fd)

        @watcher = Coolio::IOWatcher.new(@socket, "r")
        on_readable = lambda do
          receive_heartbeat
        end
        @watcher.on_readable do
          on_readable.call
        end
        @loop.attach(@watcher)
      end

      def shutdown
        @socket.close
        @watcher.detach
      end

      private
      def receive_heartbeat
        address = nil
        begin
          _, address = @socket.recvfrom(1024)
        rescue SystamCallError
          return
        end

        host = address[3]
        port = address[1]
        send_back_heartbeat(host, port)
      end

      def send_back_heartbeat(host, port)
        data = "\0"
        flags = 0
        begin
          @socket.send(data, flags, host, port)
        rescue SystemCallError
        end
      end
    end

    class Client
      include Loggable

      attr_accessor :on_close
      def initialize(io, &on_message)
        @io = io
        @on_message = on_message
        @on_close = nil
        @unpacker = MessagePack::Unpacker.new

        on_read = lambda do |data|
          feed(data)
        end
        @io.on_read do |data|
          on_read.call(data)
        end

        on_close = lambda do
          @io = nil
          @on_close.call if @on_close
        end
        @io.on_close do
          on_close.call
        end
      end

      def close
        @io.close
      end

      private
      def feed(data)
        @unpacker.feed_each(data) do |object|
          tag = object[0]
          case object[1]
          when String # PackedForward message
            entries = MessagePack.unpack(object[1])
          when Array # Forward message
            entries = object[1]
          when Integer, Float # Message message
            entries = [[object[1], object[2]]]
          else
            logger.error("unknown message", :message => object)
            next
          end
          entries.each do |time, record|
            @on_message.call(tag, time || Time.now.to_i, record)
          end
        end
      end

      def log_tag
        "fluent-message-receiver::client"
      end
    end
  end
end
