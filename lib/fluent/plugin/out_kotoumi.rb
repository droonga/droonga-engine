# -*- mode: ruby; coding: utf-8 -*-

require 'SocketIO'

module Fluent
  class KotoumiOutput < Output
    Plugin.register_output('kotoumi', self)

    require 'fluent/plugin/kotoumi'
    include Kotoumi

    config_param :database, :string, :default => "kotoumi.db"
    config_param :queuename, :string, :default => "KotoumiQueue"

    def start
      super
      # prefork @workers
      @session = Session.new(@database, @queuename)
      @outputs = {}
    end

    def shutdown
      super
      @outputs.each do |dest, socket|
        socket.disconnect
      end
    end

    def emit(tag, es, chain)
      es.each do |time, record|
        # Merge it if needed
        dispatch(tag, time, record)
      end
      chain.next
    end

    def dispatch(tag, time, record)
      # Post to peers or execute it as needed
      exec(tag, time, record)
    end

    def exec(tag, time, record)
      result = @session.process_message(tag, time, record)
      if record["replyTo"]
        post(record["replyTo"], tag, {
               inReplyTo: record["id"],
               type: (record["type"] || "") + '.result',
               body: result
             })
      end
    end

    def post(dest, tag, result)
      post_socket_io(dest, tag, result)
    end

    def post_socket_io(dest, tag, result)
      unless @outputs[dest]
        uri = 'http://' + dest
        socket = SocketIO.connect(uri, sync: true)
        @outputs[dest] = socket
      end
      @outputs[dest].emit(tag, result)
    end
  end
end
