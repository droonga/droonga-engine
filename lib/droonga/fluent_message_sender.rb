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

require "cool.io"

require "droonga/loggable"
require "droonga/message_pack_packer"

module Droonga
  class FluentMessageSender
    include Loggable

    def initialize(loop, host, port)
      @loop = loop
      @host = host
      @port = port
      @connected = false
    end

    def start
      logger.trace("start: start")
      connect
      logger.trace("start: done")
    end

    def shutdown
      logger.trace("shutdown: start")
      @socket.close unless @socket.closed?
      logger.trace("shutdown: done")
    end

    def send(tag, data)
      logger.trace("send: start")
      connect unless @connected
      fluent_message = [tag, Time.now.to_i, data]
      packed_fluent_message = MessagePackPacker.pack(fluent_message)
      @socket.write(packed_fluent_message)
      @loop.break_current_loop
      logger.trace("send: done")
    end

    private
    def connect
      logger.trace("connect: start")

      log_write_complete = lambda do
        logger.trace("write completed")
      end
      log_connect = lambda do
        logger.trace("connected to #{@host}:#{@port}")
        @connected = true
      end
      log_failed = lambda do
        logger.error("failed to connect to #{@host}:#{@port}")
      end
      on_close = lambda do
        @connected = false
      end

      @connected = false
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

    def log_tag
      "[#{Process.ppid}][#{Process.pid}] fluent-message-sender"
    end
  end
end
