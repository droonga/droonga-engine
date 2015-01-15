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
require "droonga/forward_buffer"
require "droonga/fluent_message_sender"
require "droonga/node_metadata"

module Droonga
  class EngineNode
    include Loggable

    attr_reader :name

    def initialize(name, state, sender_role, loop)
      @name  = name
      @state = state
      @sender_role = sender_role

      @buffer = ForwardBuffer.new(name)

      parsed_name = parse_node_name(@name)
      @sender = FluentMessageSender.new(loop,
                                        parsed_name[:host],
                                        parsed_name[:port],
                                        :buffering => true)
      @sender.start
    end

    def start
      logger.trace("start: start")
      @sender.resume
      @buffer.start_forward if really_writable?
      logger.trace("start: done")
    end

    def shutdown
      logger.trace("shutdown: start")
      @sender.shutdown
      logger.trace("shutdown: done")
    end

    def forward(message, destination)
      if not really_writable?
        @buffer.add(message, destination)
      elsif @buffer.empty?
        output(message, destination)
      else
        @buffer.add(message, destination)
        @buffer.start_forward
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

    def status
      if forwardable?
        "active"
      elsif dead?
        "dead"
      else
        "inactive"
      end
    end

    def to_json
      {
        "name"   => name,
        "role"   => role,
        "status" => status
      }
    end

    def on_change
      @sender.resume
    end

    private
    def parse_node_name(name)
      unless name =~ /\A(.*):(\d+)\/([^.]+)\z/
        raise "name format: hostname:port/tag"
      end
      {
        :host => $1,
        :port => $2,
        :tag  => $3,
      }
    end

    def role
      if @state
        @state["role"]
      else
        NodeMetadata::Role::SERVICE_PROVIDER
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

    def output(message, destination)
      command = destination["type"]
      receiver = destination["to"]
      arguments = destination["arguments"]
      parsed_receiver = parse_node_name(receiver)

      override_message = {
        "type" => command,
      }
      override_message["arguments"] = arguments if arguments
      message = message.merge(override_message)
      output_tag = "#{parsed_receiver[:tag]}.message"
      log_info = "<#{receiver}>:<#{output_tag}>"
      logger.trace("forward: start: #{log_info}")
      @sender.send(output_tag, message)
      logger.trace("forward: end")
    end

    def log_tag
      "[#{Process.ppid}] engine-node"
    end
  end
end
