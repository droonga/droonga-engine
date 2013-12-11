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

require "msgpack"

require "droonga/event_loop"

module Droonga
  class MessagePusher
    attr_reader :raw_receiver
    def initialize
      @loop = EventLoop.new
    end

    def start(base_path)
      socket_path = "#{base_path}.sock"
      FileUtils.rm_f(socket_path)
      @raw_receiver = UNIXServer.new(socket_path)
      FileUtils.chmod(0600, socket_path)
      @loop_thread = Thread.new do
        @loop.run
      end
    end

    def shutdown
      $log.trace("#{log_tag}: shutdown: start")
      socket_path = @raw_receiver.path
      @raw_receiver.close
      FileUtils.rm_f(socket_path)
      @loop.stop
      @loop_thread.join
      $log.trace("#{log_tag}: shutdown: done")
    end

    def push(message)
      $log.trace("#{log_tag}: push: start")
      packed_message = message.to_msgpack
      path = @raw_receiver.path
      sender = Coolio::UNIXSocket.connect(path)
      sender.write(message.to_msgpack)
      sender.on_write_complete do
        close
      end
      @loop.attach(sender)
      $log.trace("#{log_tag}: push: done")
    end

    private
    def log_tag
      "message_pusher"
    end
  end
end
