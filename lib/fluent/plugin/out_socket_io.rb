# -*- mode: ruby; coding: utf-8 -*-
#
# Copyright (C) 2013 Kotoumi project
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

require 'SocketIO'

module Fluent
  class SocketIOOutput < Output
    Plugin.register_output('socket_io', self)

    config_param :dest, :string, :default => "http://localhost"

    def configure(conf)
      super
      @socket = nil
    end

    def emit(tag, es, chain)
      @socket = SocketIO.connect(@dest, sync: true) unless @socket
      es.each do |time, record|
        @socket.emit(tag, record)
      end
      chain.next
    end
  end
end
