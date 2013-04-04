# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 droonga project
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
require "fluent-logger"
require "groonga"

require "droonga/handler_plugin"

module Droonga
  class Worker
    def initialize(database, queue_name)
      @context = Groonga::Context.new
      @database = @context.open_database(database)
      @context.encoding = :none
      @queue_name = queue_name
      @handlers = []
      @outputs = {}
      @finish = false
      @status = :IDLE
    end

    def add_handler(name)
      plugin = HandlerPlugin.new(name)
      @handlers << plugin.instantiate(@context)
    end

    def shutdown
      @handlers.each do |handler|
        handler.shutdown
      end
      @outputs.each do |dest, output|
        output[:logger].close if output[:logger]
      end
      @database.close
      @context.close
      @database = @context = nil
    end

    def start
      # TODO: doesn't work
      Signal.trap(:TERM) do
        @finish = true
        exit! 0 if @status == :IDLE
      end
      queue = @context[@queue_name]
      while !@finish
        request = nil
        queue.pull do |record|
          @status = :BUSY
          request = record.request if record
        end
        if request
          envelope = MessagePack.unpack(request)
          process_message(envelope) if request
        end
        @status = :IDLE
      end
    end

    def post_message(envelope)
      request = envelope.to_msgpack
      queue = @context[@queue_name]
      queue.push do |record|
        record.request = request
      end
    end

    def process_message(envelope)
      command = envelope["type"]
      handler = find_handler(command)
      result = handler.handle(command, envelope["body"])
      output = get_output(envelope)
      if output
        response = {
          inReplyTo: envelope["id"],
          statusCode: 200,
          type: (envelope["type"] || "") + ".result",
          body: result
        }
        output.post("message", response)
      end
    end

    private
    def find_handler(command)
      @handlers.find do |handler|
        handler.handlable?(command)
      end
    end

    def get_output(event)
      receiver = event["replyTo"]
      return nil unless receiver
      unless receiver =~ /\A(.*):(\d+)\/(.*?)(\?.+)?\z/
        raise "format: hostname:port/tag(?params)"
      end
      host = $1
      port = $2
      tag  = $3
      params = $4

      host_port = "#{host}:#{port}"
      @outputs[host_port] ||= {}
      output = @outputs[host_port]

      has_connection_id = (not params.nil? \
                           and params =~ /[\?&;]connection_id=([^&;]+)/)
      if output[:logger].nil? or has_connection_id
        connection_id = $1
        if not has_connection_id or output[:connection_id] != connection_id
          output[:connection_id] = connection_id
          logger = create_logger(tag, :host => host, :port => port.to_i)
          # output[:logger] should be closed if it exists beforehand?
          output[:logger] = logger
        end
      end

      has_client_session_id = (not params.nil? \
                               and params =~ /[\?&;]client_session_id=([^&;]+)/)
      if has_client_session_id
        client_session_id = $1
        # some generic way to handle client_session_id is expected
      end

      output[:logger]
    end

    def create_logger(tag, options)
      Fluent::Logger::FluentLogger.new(tag, options)
    end
  end
end
