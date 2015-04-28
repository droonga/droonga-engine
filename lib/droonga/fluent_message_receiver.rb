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

    class InvalidObject < StandardError
      def initialize(object)
        message = "no valid tag information"
        super(message, :object => obejct)
      end
    end

    def initialize(loop, options={}, &on_message)
      @loop = loop
      @listen_fd = options[:listen_fd]
      @heartbeat_fd = options[:heartbeat_fd]
      @server = nil
      @clients = []
      @on_message = on_message
      @on_shutdown_ready = nil
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

    def ensure_no_client(&block)
      if @clients.empty?
        logger.trace("ensure_no_client: no client")
        yield
      elsif block_given?
        logger.trace("ensure_no_client: waiting for #{@clients.size} clients to be disconnected",
                     :clients => @clients)
        @on_shutdown_ready = lambda do
          logger.trace("ensure_no_client: all clients are disconnected")
          yield
        end
      end
    end

    def stop_immediately
      logger.trace("stop_immediately: start")
      stop_gracefully
      force_shutdown_clients
      logger.trace("stop_immediately: done")
    end

    private
    def force_shutdown_clients
      @clients.dup.each do |client|
        client.close
      end
    end

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
        logger.trace("Client: new connection", :connection => connection)
        client = Client.new(connection) do |tag, time, record|
          logger.trace("Client: on_message: start")
          @on_message.call(tag, time, record)
          logger.trace("Client: on_message: done")
        end
        client.on_close = lambda do
          @clients.delete(client)
          if @on_shutdown_ready
            logger.trace("Client: a client is disconnected. still waiting for #{@clients.size} clients.",
                         :clients => @clients)
            if @clients.empty?
              @on_shutdown_ready.call
            end
          end
        end
        @clients << client
      end
      @loop.attach(@server)
      logger.trace("start_server: server watcher attached",
                   :watcher      => @server,
                   :listen_fd    => @listen_fd,
                   :heartbeat_fd => @heartbeat_fd)

      logger.trace("start_server: done")
    end

    def create_server(&block)
      Coolio::Server.new(TCPServer.for_fd(@listen_fd), Coolio::TCPSocket, &block)
    end

    def shutdown_server
      logger.trace("shutdown_server: start")
      @server.close
      logger.trace("shutdown_server: server watcher detached",
                   :watcher => @server)
      logger.trace("shutdown_server: done")
    end

    def log_tag
      "fluent-message-receiver"
    end

    class HeartbeatReceiver
      include Loggable

      def initialize(loop, fd)
        @loop = loop
        @fd = fd
      end

      def start
        @socket = UDPSocket.for_fd(@fd)

        @watcher = Coolio::IOWatcher.new(@socket, "r")
        @watcher.on_readable do
          receive_heartbeat
        end
        @loop.attach(@watcher)
        logger.trace("start: heartbeat IO watcher attached",
                     :watcher => @watcher,
                     :fd      => @fd)
      end

      def shutdown
        @socket.close
        @watcher.detach
        logger.trace("shutdown: heartbeat watcher detached",
                     :watcher => @watcher,
                     :fd      => @fd)
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

      def log_tag
        "heartbeat-receiver"
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

        @io.on_read do |data|
          feed(data)
        end

        @io.on_close do
          @io = nil
          @on_close.call if @on_close
        end
      end

      def close
        @io.close
      end

      private
      def feed(data)
        @unpacker.feed_each(data) do |object|
          logger.trace("Client: feed_each: start", :object => object)
          begin
          raise InvalidObject.new(object) unless object.is_a?(Array)
          tag = object[0]
          raise InvalidObject.new(object) unless tag.is_a?(String)
          case object[1]
          when String # PackedForward message
            raise InvalidObject.new(object) unless object.size == 2
            entries = MessagePack.unpack(object[1])
          when Array # Forward message
            raise InvalidObject.new(object) unless object.size == 2
            entries = object[1]
          when Integer, Float # Message message
            raise InvalidObject.new(object) unless object.size == 3
            entries = [[object[1], object[2]]]
          else
            logger.error("unknown type message: couldn't detect entries",
                         :message => object)
            next
          end
          raise InvalidObject.new(object) unless entries.is_a?(Array)
          entries.each do |time, record|
            @on_message.call(tag, time || Time.now.to_i, record)
          end
          rescue InvalidObject => error
            logger.error("invalid object received", :object => object)
          end
          logger.trace("Client: feed_each: done")
        end
      end

      def log_tag
        "fluent-message-receiver::client"
      end
    end
  end
end
