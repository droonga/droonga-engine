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

require "socket"
require "msgpack"
require "kotoumi/worker"

module Fluent
  class KotoumiOutput < Output
    Plugin.register_output("kotoumi", self)

    config_param :database, :string, :default => "kotoumi.db"
    config_param :queue_name, :string, :default => "KotoumiQueue"

    def start
      super
      # prefork @workers
      @worker = Kotoumi::Worker.new(@database, @queue_name)
      @outputs = {}
    end

    def shutdown
      super
      @outputs.each do |dest, socket|
        socket.close
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
      result = @worker.process_message(record)
      if record["replyTo"]
        post(record["replyTo"], tag, {
               inReplyTo: record["id"],
               type: (record["type"] || "") + ".result",
               body: result
             })
      end
    end

    def post(dest, tag, result)
      unless @outputs[dest]
        host, port = dest.split(/:/, 2)
        port = Integer(port)
        socket = TCPSocket.new(host, port)
        @outputs[dest] = socket
      end
      data = {"tag" => tag, "data" => result}.to_msgpack
      @outputs[dest].write(data)
    end
  end
end
