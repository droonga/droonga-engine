# Copyright (C) 2013-2014 Droonga Project
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

require "droonga/loggable"
require "droonga/job_message_protocol"

module Droonga
  class MessageReceiver
    include Loggable

    def initialize(loop, socket_path, &callback)
      @loop = loop
      @socket_path = socket_path
      @callback = callback
    end

    def start
      logger.trace("start: start")
      @receiver = Coolio::UNIXSocket.connect(@socket_path)
      setup_receive_handler(@receiver)
      @loop.attach(@receiver)
      logger.trace("start: done")
    end

    def shutdown
      logger.trace("shutdown: start")
      @receiver.close
      logger.trace("shutdown: done")
    end

    private
    def setup_receive_handler(connection)
      unpacker = MessagePack::Unpacker.new
      on_read = lambda do |data|
        logger.trace("on_read: start")
        unpacker.feed_each(data) do |message|
          @callback.call(message)
        end
        logger.trace("on_read: done")
        send_ready(connection)
      end
      connection.on_read do |data|
        on_read.call(data)
      end
      send_ready(connection)
    end

    def send_ready(connection)
      connection.write(JobMessageProtocol::READY_SIGNAL)
    end

    def log_tag
      "message_receiver"
    end
  end
end
