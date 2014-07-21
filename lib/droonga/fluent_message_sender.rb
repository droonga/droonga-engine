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

require "fileutils"
require "thread"

require "cool.io"

require "droonga/message-pack-packer"

require "droonga/loggable"
require "droonga/buffered_tcp_socket"

module Droonga
  class FluentMessageSender
    include Loggable

    def initialize(loop, host, port, options={})
      @loop = loop
      @host = host
      @port = port
      @socket = nil
      @buffering = options[:buffering]
    end

    def start
      logger.trace("start: start")
      logger.trace("start: done")
    end

    def shutdown
      logger.trace("shutdown: start")
      shutdown_socket
      logger.trace("shutdown: done")
    end

    def send(tag, data)
      logger.trace("send: start")
      packed_fluent_message = create_packed_fluent_message(tag, data)
      connect unless connected?
      @socket.write(packed_fluent_message)
      logger.trace("send: done")
    end

    def resume
      return unless @buffering
      connect unless connected?
      @socket.resume
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

      if @buffering
        data_directory = Path.buffer + "#{@host}:#{@port}"
        FileUtils.mkdir_p(data_directory.to_s)
        @socket = BufferedTCPSocket.connect(@host, @port, data_directory)
      else
        @socket = Coolio::TCPSocket.connect(@host, @port)
      end
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

    def create_packed_fluent_message(tag, data)
      fluent_message = [tag, Time.now.to_i, data]
      MessagePackPacker.pack(fluent_message)
    end

    def log_tag
      "[#{Process.ppid}] fluent-message-sender"
    end
  end
end
