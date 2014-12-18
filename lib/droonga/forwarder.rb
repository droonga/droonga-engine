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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "droonga/loggable"
require "droonga/path"
require "droonga/event_loop"
require "droonga/buffered_tcp_socket"
require "droonga/forward_buffer"
require "droonga/fluent_message_sender"

module Droonga
  class Forwarder
    include Loggable

    def initialize(loop, options={})
      @loop = loop
      @buffering = options[:buffering]
      @engine_state = options[:engine_state]
      @buffers = {}
      @senders = {}
    end

    def start
      logger.trace("start: start")
      resume
      logger.trace("start: done")
    end

    def shutdown
      logger.trace("shutdown: start")
      @senders.each_value do |sender|
        sender.shutdown
      end
      logger.trace("shutdown: done")
    end

    def forward(message, destination)
      logger.trace("forward: start")
      command = destination["type"]
      receiver = destination["to"]
      arguments = destination["arguments"]
      buffered_output(receiver, message, command, arguments)
      logger.trace("forward: done")
    end

    def resume
      resume_from_accidents
    end

    def resume_from_accidents
      return unless Path.accidental_buffer.exist?
      Pathname.glob("#{Path.accidental_buffer}/*") do |path|
        next unless path.directory?

        destination = path.basename.to_s
        sender = @senders[destination]
        if sender
          sender.resume
          next
        end

        chunk_loader = BufferedTCPSocket::ChunkLoader.new(path)
        unless chunk_loader.have_any_chunk?
          #FileUtils.rm_rf(path.to_s) # TODO re-enable this
          next
        end

        components = destination.split(":")
        port = components.pop.to_i
        next if port.zero?
        host = components.join(":")

        sender = create_sender(host, port)
        sender.resume
        @senders[destination] = sender
      end
    end

    def output(receiver, message, command, arguments, options={})
      logger.trace("output: start")
      if not receiver.is_a?(String) or not command.is_a?(String)
        logger.trace("output: abort: invalid argument",
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
        logger.trace("output: abort: no sender",
                     :host   => host,
                     :port   => port,
                     :params => params)
        return
      end
      override_message = {
        "type" => command,
      }
      override_message["arguments"] = arguments if arguments
      message = message.merge(override_message)
      output_tag = "#{tag}.message"
      log_info = "<#{receiver}>:<#{output_tag}>"
      logger.trace("output: post: start: #{log_info}")
      sender.send(output_tag, message)
      logger.trace("output: post: done: #{log_info}")
      logger.trace("output: done")
    end

    private
    def buffered_output(receiver, message, command, arguments, options={})
      receiver_is_node = (receiver =~ /\A([^:]+:\d+)/)
      node_name = $1
      unless receiver_is_node
        output(receiver, message, command, arguments, options)
        return
      end
      
      buffer = buffer_for(node_name)
      if @engine_state and
           @engine_state.unwritable_node?(node_name)
        buffer.add(receiver, message, command, arguments, options)
      elsif buffer.empty?
        output(receiver, message, command, arguments, options)
      else
        buffer.add(receiver, message, command, arguments, options)
        buffer.resume
      end
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
      options = {
        :buffering => @buffering,
      }
      sender = FluentMessageSender.new(@loop, host, port, options)
      sender.start
      sender
    end

    def buffer_for(node_name)
      @buffers[node_name] ||= ForwardBuffer.new(node_name,
                                                :forwarder => self)
    end

    def log_tag
      "[#{Process.ppid}] forwarder"
    end
  end
end
