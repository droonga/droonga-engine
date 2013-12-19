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

require "droonga/event_loop"
require "droonga/fluent_message_sender"

module Droonga
  class Forwarder
    def initialize(loop)
      @loop = loop
      @senders = {}
    end

    def start
      $log.trace("#{log_tag}: start: start")
      $log.trace("#{log_tag}: start: done")
    end

    def shutdown
      $log.trace("#{log_tag}: shutdown: start")
      @senders.each_value do |sender|
        sender.shutdown
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
      sender = find_sender(host, port, params)
      unless sender
        $log.trace("#{log_tag}: output: abort: no sender",
                   :host   => host,
                   :port   => port,
                   :params => params)
        return
      end
      if command =~ /\.result$/
        message = {
          "inReplyTo" => envelope["id"],
          "statusCode" => 200,
          "type" => command,
          "body" => body
        }
      else
        message = envelope.merge(
          "body" => body,
          "type" => command,
          "arguments" => arguments
        )
      end
      output_tag = "#{tag}.message"
      log_info = "<#{receiver}>:<#{output_tag}>"
      $log.trace("#{log_tag}: output: post: start: #{log_info}")
      sender.send(output_tag, message)
      $log.trace("#{log_tag}: output: post: done: #{log_info}")
      $log.trace("#{log_tag}: output: done")
    end

    def find_sender(host, port, params)
      connection_id = extract_connection_id(params)
      destination = "#{host}:#{port}"
      destination << "?#{connection_id}" if connection_id

      @senders[destination] ||= create_sender(host, port)
    end

    def extract_connection_id(params)
      return nil unless params

      if /[\?&;]connection_id=([^&;]+)/ =~ params
        $1
      else
        nil
      end
    end

    def create_sender(host, port)
      sender = FluentMessageSender.new(@loop, host, port)
      sender.start
      sender
    end

    def log_tag
      "[#{Process.ppid}][#{Process.pid}] forwarder"
    end
  end
end
