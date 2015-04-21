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

require "time"

require "droonga/loggable"
require "droonga/forward_buffer"
require "droonga/fluent_message_sender"
require "droonga/node_role"

module Droonga
  class EngineNode
    include Loggable

    attr_reader :name

    def initialize(name, state, loop, params)
      @name  = name
      logger.trace("initialize: start")

      @state = state

      @buffer = ForwardBuffer.new(name)
      boundary_timestamp = accept_messages_newer_than_timestamp
      @buffer.process_messages_newer_than(boundary_timestamp)
      @buffer.on_forward = lambda do |message, destination|
        output(message, destination)
      end

      parsed_name = parse_node_name(@name)
      @sender = FluentMessageSender.new(loop,
                                        parsed_name[:host],
                                        parsed_name[:port],
                                        :buffering => true)
      @sender.start
      logger.trace("initialize: done")
    end

    def start
      logger.trace("start: start")
      resume
      logger.trace("start: done")
    end

    def shutdown
      logger.trace("shutdown: start")
      @sender.shutdown
      logger.trace("shutdown: done")
    end

    def forward(message, destination)
      if read_message?(message)
        # A node can receive read messages for other nodes,
        # while changing its role. They must not be buffered.
        output(message, destination)
        return
      end

      unless really_writable?
        # The target node is not ready. We should send the message later.
        @buffer.add(message, destination)
        return
      end

      # The target node is ready.
      if @buffer.empty?
        output(message, destination)
      else
        @buffer.add(message, destination)
        @buffer.start_forward
      end
    end

    def forwardable?
      return false unless live?
      role == NodeRole.my_role
    end

    def readable?
      forwardable? and @buffer.empty? and
        (complete_service_provider? or not service_provider?)
    end

    def writable?
      case NodeRole.my_role
      when NodeRole::SERVICE_PROVIDER
        true
      when NodeRole::ABSORB_SOURCE
        absorb_source?
      when NodeRole::ABSORB_DESTINATION
        absorb_destination?
      else
        false
      end
    end

    def status
      if readable?
        "active"
      elsif forwardable?
        "inactive"
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
        "live"   => live?,
        "status" => status,
      }
    end

    def resume
      logger.trace("resume: start")
      @sender.resume
      unless @buffer.empty?
        if really_writable?
          logger.info("Target becomes writable. Start to forwarding.")
          @buffer.start_forward
        else
          logger.info("Target is still unwritable.")
        end
      end
      logger.trace("resume: done")
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
        NodeRole::SERVICE_PROVIDER
      end
    end

    def live?
      @state.nil? or @state["live"]
    end

    def have_unprocessed_messages?
      @state and @state["have_unprocessed_messages"]
    end

    def accept_messages_newer_than_timestamp
      @accept_messages_newer_than_timestamp ||= parse_accept_messages_newer_than_timestamp
    end

    def parse_accept_messages_newer_than_timestamp
      return nil if @state.nil? or @state["accept_messages_newer_than"].nil?
      Time.parse(@state["accept_messages_newer_than"])
    end

    def dead?
      not live?
    end

    def service_provider?
      role == NodeRole::SERVICE_PROVIDER
    end

    def absorb_source?
      role == NodeRole::ABSORB_SOURCE
    end

    def absorb_destination?
      role == NodeRole::ABSORB_DESTINATION
    end

    def complete_service_provider?
      service_provider? and not have_unprocessed_messages?
    end

    def really_writable?
      return false unless writable?
      case NodeRole.my_role
      when NodeRole::SERVICE_PROVIDER
        service_provider?
      when NodeRole::ABSORB_SOURCE
        not absorb_destination?
      else
        true
      end
    end

    def read_message?(message)
      steps = message["body"]["steps"]
      return false unless steps
      steps.all? do |step|
        not step["write"]
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
      "[#{Process.ppid}] engine-node: #{@name}"
    end
  end
end
