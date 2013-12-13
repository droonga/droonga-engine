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

require "fluent-logger"
require "fluent/logger/fluent_logger"

module Droonga
  class Forwarder
    def initialize
      @outputs = {}
    end

    def start
      $log.trace("#{log_tag}: start: start")
      $log.trace("#{log_tag}: start: done")
    end

    def shutdown
      $log.trace("#{log_tag}: shutdown: start")
      @outputs.each do |dest, output|
        output[:logger].close if output[:logger]
      end
      $log.trace("#{log_tag}: shutdown: done")
    end

    def forward(envelope, body, destination)
      $log.trace("#{log_tag}: forward: start")
      command = destination["type"]
      receiver = destination["to"]
      arguments = destination["arguments"]
      output(receiver, envelope, body, command, arguments)
      $log.trace("#{log_tag}: forward: done")
    end

    private
    def output(receiver, envelope, body, command, arguments)
      $log.trace("#{log_tag}: output: start")
      unless receiver.is_a?(String) && command.is_a?(String)
        $log.trace("#{log_tag}: output: abort: invalid argument",
                   :receiver => receiver,
                   :command  => command)
        return
      end
      unless receiver =~ /\A(.*):(\d+)\/(.*?)(\?.+)?\z/
        raise "format: hostname:port/tag(?params)"
      end
      host = $1
      port = $2
      tag  = $3
      params = $4
      output = get_output(host, port, params)
      unless output
        $log.trace("#{log_tag}: output: abort: no output",
                   :host   => host,
                   :port   => port,
                   :params => params)
        return
      end
      if command =~ /\.result$/
        message = {
          inReplyTo: envelope["id"],
          statusCode: 200,
          type: command,
          body: body
        }
      else
        message = envelope.merge(
          body: body,
          type: command,
          arguments: arguments
        )
      end
      output_tag = "#{tag}.message"
      log_info = "<#{receiver}>:<#{output_tag}>"
      $log.trace("#{log_tag}: output: post: start: #{log_info}")
      output.post(output_tag, message)
      $log.trace("#{log_tag}: output: post: done: #{log_info}")
      $log.trace("#{log_tag}: output: done")
    end

    def get_output(host, port, params)
      host_port = "#{host}:#{port}"
      @outputs[host_port] ||= {}
      output = @outputs[host_port]

      has_connection_id = (not params.nil? \
                           and params =~ /[\?&;]connection_id=([^&;]+)/)
      if output[:logger].nil? or has_connection_id
        connection_id = $1
        if not has_connection_id or output[:connection_id] != connection_id
          output[:connection_id] = connection_id
          logger = create_logger(:host => host, :port => port.to_i)
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

    def create_logger(options)
      Fluent::Logger::FluentLogger.new(nil, options)
    end

    def log_tag
      "[#{Process.ppid}][#{Process.pid}] forwarder"
    end
  end
end
