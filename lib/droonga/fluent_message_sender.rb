# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Droonga Project
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

require "thread"

require "cool.io"

require "droonga/message-pack-packer"

require "droonga/loggable"

module Droonga
  class FluentMessageSender
    include Loggable

    def initialize(loop, host, port)
      @loop = loop
      @host = host
      @port = port
      @socket = nil
      @buffer = []
      @write_mutex = Mutex.new
    end

    def start
      logger.trace("start: start")
      start_writer
      logger.trace("start: done")
    end

    def shutdown
      logger.trace("shutdown: start")
      shutdown_writer
      shutdown_socket
      logger.trace("shutdown: done")
    end

    def send(tag, data)
      logger.trace("send: start")
      fluent_message = [tag, Time.now.to_i, data]
      packed_fluent_message = MessagePackPacker.pack(fluent_message)
      @write_mutex.synchronize do
        @buffer << packed_fluent_message
        @writer.signal
      end
      logger.trace("send: done")
    end

    private
    def connected?
      not @socket.nil?
    end

    def connect
      logger.trace("connect: start")

      log_write_complete = lambda do
        logger.trace("write completed")
      end
      log_connect = lambda do
        logger.trace("connected to #{@host}:#{@port}")
      end
      log_failed = lambda do
        logger.error("failed to connect to #{@host}:#{@port}")
        @socket = nil
      end
      on_close = lambda do
        @socket = nil
      end

      @socket = Coolio::TCPSocket.connect(@host, @port)
      @socket.on_write_complete do
        log_write_complete.call
      end
      @socket.on_connect do
        log_connect.call
      end
      @socket.on_connect_failed do
        log_failed.call
      end
      @socket.on_close do
        on_close.call
      end
      @loop.attach(@socket)

      logger.trace("connect: done")
    end

    def shutdown_socket
      return unless connected?
      @socket.close unless @socket.closed?
    end

    def start_writer
      @writer = Coolio::AsyncWatcher.new

      on_signal = lambda do
        @write_mutex.synchronize do
          connect unless connected?
          @buffer.each do |data|
            @socket.write(data)
          end
          @buffer.clear
        end
      end
      @writer.on_signal do
        on_signal.call
      end

      @loop.attach(@writer)
    end

    def shutdown_writer
      @writer.detach
    end

    def log_tag
      "[#{Process.ppid}][#{Process.pid}] fluent-message-sender"
    end
  end
end
