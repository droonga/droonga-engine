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

require "droonga/logger"

module Droonga
  class MessagePusher
    include Loggable

    attr_reader :raw_receiver
    def initialize(loop)
      @loop = loop
    end

    def start(base_path)
      socket_path = "#{base_path}.sock"
      FileUtils.rm_f(socket_path)
      @raw_receiver = UNIXServer.new(socket_path)
      FileUtils.chmod(0600, socket_path)
    end

    def shutdown
      logger.trace("shutdown: start")
      socket_path = @raw_receiver.path
      @raw_receiver.close
      FileUtils.rm_f(socket_path)
      logger.trace("shutdown: done")
    end

    def push(message)
      logger.trace("push: start")
      packed_message = message.to_msgpack
      path = @raw_receiver.path
      sender = Coolio::UNIXSocket.connect(path)
      sender.write(message.to_msgpack)
      sender.on_write_complete do
        close
      end
      @loop.attach(sender)
      logger.trace("push: done")
    end

    private
    def log_tag
      "message_pusher"
    end
  end
end
