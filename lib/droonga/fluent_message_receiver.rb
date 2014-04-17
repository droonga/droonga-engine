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

require "socket"
require "ipaddr"

require "msgpack"

require "droonga/loggable"
require "droonga/event_loop"

module Droonga
  class FluentMessageReceiver
    include Loggable

    def initialize(loop, options={}, &on_message)
      @loop = loop
      @host = options[:host] || "0.0.0.0"
      @port = options[:port] || 24224
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

    def shutdown
      logger.trace("shutdown: start")
      shutdown_server
      shutdown_clients
      shutdown_heartbeat_receiver
      logger.trace("shutdown: done")
    end

    private
    def start_heartbeat_receiver
      logger.trace("start_heartbeat_receiver: start")
      @heartbeat_receiver = HeartbeatReceiver.new(@loop, @host, @port)
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
      @server = Coolio::TCPServer.new(@host, @port) do |connection|
        client = Client.new(connection) do |tag, time, record|
          @on_message.call(tag, time, record)
        end
        @clients << client
      end
      @loop.attach(@server)

      logger.trace("start_server: done")
    end

    def shutdown_server
      @server.close
    end

    def shutdown_clients
      @clients.each do |client|
        client.close
      end
    end

    def log_tag
      "fluent-message-receiver"
    end

    class HeartbeatReceiver
      def initialize(loop, host, port)
        @loop = loop
        @host = host
        @port = port
      end

      def start
        @socket = UDPSocket.new(address_family)
        @socket.bind(@host, @port)

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
        @watcher.detach
        @socket.close
      end

      private
      def address_family
        ip_address = IPAddr.new(IPSocket.getaddress(@host))
        ip_address.family
      end

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

      def initialize(io, &on_message)
        @io = io
        @on_message = on_message
        @unpacker = MessagePack::Unpacker.new
        on_read = lambda do |data|
          feed(data)
        end
        @io.on_read do |data|
          on_read.call(data)
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
          when Integer # Message message
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
