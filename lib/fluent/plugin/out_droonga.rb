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

require "fluent-logger"
require "droonga/worker"

module Fluent
  class DroongaOutput < Output
    Plugin.register_output("droonga", self)

    config_param :database, :string, :default => "droonga.db"
    config_param :queue_name, :string, :default => "DroongaQueue"

    def start
      super
      # prefork @workers
      @worker = create_worker
      @outputs = {}
    end

    def shutdown
      super
      @worker.shutdown
      @outputs.each do |dest, output|
        output[:logger].close if output[:logger]
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

    def exec(tag, time, record)
      result = @worker.process_message(record)
      output = get_output(record)
      if output
        response = {
          inReplyTo: record["id"],
          statusCode: 200,
          type: (record["type"] || "") + ".result",
          body: {
            result: result
          }
        }
        output.post("message", response)
      end
    end

    private
    def create_worker
      Droonga::Worker.new(@database, @queue_name)
    end

    def create_logger(tag, options)
      Fluent::Logger::FluentLogger.new(tag, options)
    end
  end
end
