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
  class MessageReceiver
    def initialize(receiver, &callback)
      @loop = EventLoop.new
      @receiver = Coolio::Server.new(receiver, Coolio::Socket) do |connection|
        setup_receive_handler(connection)
      end
      @loop.attach(@receiver)
      @callback = callback
    end

    def run
      @loop.run
    end

    def stop
      $log.trace("#{log_tag}: shutdown: start")
      @receiver.close
      @loop.stop
      $log.trace("#{log_tag}: shutdown: done")
    end

    private
    def setup_receive_handler(connection)
      unpacker = MessagePack::Unpacker.new
      on_read = lambda do |data|
        $log.trace("#{log_tag}: on_read: start")
        unpacker.feed_each(data) do |message|
          @callback.call(message)
        end
        $log.trace("#{log_tag}: on_read: done")
      end
      connection.on_read do |data|
        on_read.call(data)
      end
    end

    def log_tag
      "message_puller"
    end
  end
end
