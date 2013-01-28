# -*- coding: utf-8 -*-
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
  class KotoumiOutput < Output
    Plugin.register_output('kotoumi', self)

    require 'kotoumi/worker'

    config_param :database, :string, :default => "kotoumi.db"
    config_param :queuename, :string, :default => "KotoumiQueue"

    def start
      super
      # prefork @workers
      @session = Kotoumi::Worker.new(@database, @queuename)
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
