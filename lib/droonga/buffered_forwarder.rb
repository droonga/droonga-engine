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
require "droonga/forwarder"
require "droonga/forward_buffer"

module Droonga
  class BufferedForwarder
    include Loggable

    def initialize(loop, options={})
      @loop = loop
      @options = options
      @buffering = options[:buffering]
      @engine_state = options[:engine_state]
      @buffers = {}
      @forwarder = Forwarder.new(loop, options)
    end

    def start
      logger.trace("start: start")
      resume
      logger.trace("start: done")
    end

    def shutdown
      logger.trace("shutdown: start")
      @forwarder.shutdown
      logger.trace("shutdown: done")
    end

    def resume
      @forwarder.resume
      @buffers.each do |node_name, buffer|
        if writable_node?(node_name)
          buffer.start_forward
        end
      end
    end

    def forward(message, destination)
      logger.trace("forward: start")
      unless @buffering
        @forwarder.forward(message, destination)
        return
      end

      receiver = destination["to"]
      receiver_is_node = (receiver =~ /\A([^:]+:\d+\/[^\.]+)/)
      node_name = $1
      unless receiver_is_node
        @forwarder.forward(message, destination)
        return
      end

      buffer = @buffers[node_name] ||= ForwardBuffer.new(node_name, @forwarder)

      if writable_node?(node_name)
        buffer.add(message, destination)
      elsif buffer.empty?
        @forwarder.forward(message, destination)
      else
        buffer.add(message, destination)
        buffer.start_forward
      end

      logger.trace("forward: done")
    end

    private
    def writable_node?(node_name)
      @engine_state and
        @engine_state.unwritable_node?(node_name)
    end

    def log_tag
      "[#{Process.ppid}] buffered-forwarder"
    end
  end
end
