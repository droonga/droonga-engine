# Copyright (C) 2015 Droonga Project
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
require "droonga/node_metadata"

module Droonga
  class EngineNode
    attr_reader :name, :forwarder

    def initialize(name, state, sender_role, loop)
      @name  = name
      @state = state
      @sender_role = sender_role

      @forwarder = Forwarder.new(loop, :buffering => true)
      @buffer = ForwardBuffer.new(name, @forwarder)
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
      @buffer.start_forward if really_writable?
    end

    def forward(message, destination)
      if not really_writable?
        @buffer.add(message, destination)
      elsif @buffer.empty?
        @forwarder.forward(message, destination)
      else
        @buffer.add(message, destination)
        @buffer.start_forward
      end
    end

    def live?
      @state.nil? or @state["live"]
    end

    def dead?
      not live?
    end

    def service_provider?
      role == NodeMetadata::Role::SERVICE_PROVIDER
    end

    def absorb_source?
      role == NodeMetadata::Role::ABSORB_SOURCE
    end

    def absorb_destination?
      role == NodeMetadata::Role::ABSORB_DESTINATION
    end

    def role
      if @state
        @state["role"]
      else
        NodeMetadata::Role::SERVICE_PROVIDER
      end
    end

    def forwardable?
      return false unless live?
      role == @sender_role
    end

    def writable?
      case @sender_role
      when NodeMetadata::Role::SERVICE_PROVIDER
        true
      when NodeMetadata::Role::ABSORB_SOURCE
        absorb_source?
      when NodeMetadata::Role::ABSORB_DESTINATION
        absorb_destination?
      else
        false
      end
    end

    def really_writable?
      return false unless writable?
      case @sender_role
      when NodeMetadata::Role::SERVICE_PROVIDER
        service_provider?
      when NodeMetadata::Role::ABSORB_SOURCE
        not absorb_destination?
      else
        true
      end
    end

    def status
      if forwardable?
        "active"
      elsif dead?
        "dead"
      else
        "inactive"
      end
    end

    private
    def on_change
      @forwarder.resume
    end

    def log_tag
      "[#{Process.ppid}] engine-node"
    end
  end
end
