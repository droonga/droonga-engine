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
require "droonga/message_pack_packer"

module Droonga
  class FluentMessageSender
    def initialize(loop, host, port)
      @loop = loop
      @host = host
      @port = port
    end

    def start
      $log.trace("#{log_tag}: start: start")
      connect
      $log.trace("#{log_tag}: start: done")
    end

    def shutdown
      $log.trace("#{log_tag}: shutdown: start")
      @socket.close unless @socket.closed?
      $log.trace("#{log_tag}: shutdown: done")
    end

    def send(tag, data)
      $log.trace("#{log_tag}: send: start")
      connect if @socket.closed?
      fluent_message = MessagePackPacker.to_msgpack(
        [tag, Time.now.to_i, data]
      )
      @socket.write(fluent_message)
      @loop.break_current_loop
      $log.trace("#{log_tag}: send: done")
    end

    private
    def connect
      $log.trace("#{log_tag}: connect: start")

      log_write_complete = lambda do
        $log.trace("#{log_tag}: write completed")
      end
      log_connect = lambda do
        $log.trace("#{log_tag}: connected to #{@host}:#{@port}")
      end
      log_failed = lambda do
        $log.error("#{log_tag}: failed to connect to #{@host}:#{@port}")
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
      @loop.attach(@socket)

      $log.trace("#{log_tag}: connect: done")
    end

    def log_tag
      "[#{Process.ppid}][#{Process.pid}] fluent-message-sender"
    end
  end
end
