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

require "droonga/fluent_message_receiver"

module Droonga
  class InternalFluentMessageReceiver
    include Loggable

    def initialize(loop, host, &on_message)
      @loop = loop
      @host = host
      @on_message = on_message
    end

    def start
      logger.trace("start: start")
      start_listen_socket
      start_heartbeat_socket
      start_message_receiver
      logger.trace("start: done")

      [@host, @port]
    end

    def shutdown
      logger.trace("shutdown: start")
      shutdown_message_receiver
      shutdown_heartbeat_socket
      shutdown_listen_socket
      logger.trace("shutdown: done")
    end

    private
    def start_listen_socket
      logger.trace("start_listen_socket: start")
      @listen_socket = TCPServer.new(@host, 0)
      @port = @listen_socket.addr[1]
      logger.trace("start_listen_socket: done")
    end

    def shutdown_listen_socket
      logger.trace("shutdown_listen_socket: start")
      logger.trace("shutdown_listen_socket: done")
    end

    def address_family
      ip_address = IPAddr.new(IPSocket.getaddress(@host))
      ip_address.family
    end

    def start_heartbeat_socket
      logger.trace("start_heartbeat_socket: start")
      @heartbeat_socket = UDPSocket.new(address_family)
      @heartbeat_socket.bind(@host, @port)
      logger.trace("start_heartbeat_socket: done")
    end

    def shutdown_heartbeat_socket
      logger.trace("shutdown_heartbeat_socket: start")
      logger.trace("shutdown_heartbeat_socket: done")
    end

    def start_message_receiver
      logger.trace("start_heartbeat_socket: start")
      options = {
        :listen_fd    => @listen_socket.fileno,
        :heartbeat_fd => @heartbeat_socket.fileno,
      }
      @message_receiver = FluentMessageReceiver.new(@loop, options, &@on_message)
      @message_receiver.start
      logger.trace("start_heartbeat_socket: done")
    end

    def shutdown_message_receiver
      logger.trace("shutdown_message_receiver: start")
      @message_receiver.shutdown
      logger.trace("shutdown_message_receiver: done")
    end

    def log_tag
      "internal-fluent-message-receiver"
    end
  end
end
