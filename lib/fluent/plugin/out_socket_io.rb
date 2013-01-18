# -*- mode: ruby; coding: utf-8 -*-

require 'SocketIO'

module Fluent
  class SocketIOOutput < Output
    Plugin.register_output('socket_io', self)

    config_param :dest, :string, :default => "http://localhost:3000"

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
