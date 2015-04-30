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
require "coolio"

require "droonga/loggable"
require "droonga/forward_buffer"
require "droonga/fluent_message_sender"
require "droonga/node_name"
require "droonga/node_role"

module Droonga
  class EngineNode
    include Loggable

    DEFAULT_AUTO_CLOSE_TIMEOUT_SECONDS = 60

    attr_reader :name

    def initialize(params)
      @loop  = params[:loop]
      @name  = params[:name]
      @state = params[:state]
      logger.trace("initialize: start")

      @buffer = ForwardBuffer.new(name)
      boundary_timestamp = accept_messages_newer_than_timestamp
      @buffer.process_messages_newer_than(boundary_timestamp)
      @buffer.on_forward = lambda do |message, destination|
        output(message, destination)
      end

      @node_name = NodeName.parse(@name)

      @sender = nil
      @auto_close_timer = nil
      @auto_close_timeout = params[:auto_close_timeout] ||
                              DEFAULT_AUTO_CLOSE_TIMEOUT_SECONDS

      logger.trace("initialize: done")
    end

    def start
      logger.trace("start: start")
      resume
      logger.trace("start: done")
    end

    def shutdown
      logger.trace("shutdown: start")
      if @sender
        @sender.shutdown
        @sender = nil
      end
      if @auto_close_timer
        @auto_close_timer.detach
        @auto_close_timer = nil
      end
      logger.trace("shutdown: done")
    end

    def refresh_connection
      logger.trace("refresh_connection: start")
      shutdown
      sender # instantiate new sender
      logger.trace("refresh_connection: done")
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

    def bounce(message)
      destination = {
        "to"   => name,
        "type" => message["type"],
      }
      output(message, destination)
    end

    def role
      if @state
        NodeRole.normalize(@state["role"])
      else
        NodeRole::SERVICE_PROVIDER
      end
    end

    def live?
      @state.nil? or
        @state["live"] == true
    end

    def forwardable?
      return false unless live?
      role == NodeRole.mine
    end

    def readable?
      forwardable? and @buffer.empty? and
        (complete_service_provider? or not service_provider?)
    end

    def writable?
      case NodeRole.mine
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
      sender.resume
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
      case NodeRole.mine
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
      parsed_receiver = NodeName.parse(receiver)

      override_message = {
        "type" => command,
      }
      override_message["arguments"] = arguments if arguments
      message = message.merge(override_message)
      output_tag = "#{parsed_receiver.tag}.message"
      log_info = "<#{receiver}>:<#{output_tag}>"
      logger.trace("forward: start: #{log_info}")
      sender.send(output_tag, message)
      set_auto_close_timer
      logger.trace("forward: end")
    end

    def sender
      @sender ||= create_sender
    end

    def create_sender
      sender = FluentMessageSender.new(@loop,
                                       @node_name.host,
                                       @node_name.port,
                                       :buffering => true)
      sender.start
      sender
    end

    def set_auto_close_timer
      previous_timer = @auto_close_timer
      previous_timer.detach if previous_timer

      @auto_close_timer = Coolio::TimerWatcher.new(@auto_close_timeout)
      @auto_close_timer.on_timer do
        @auto_close_timer.detach
        @auto_close_timer = nil
        if @sender
          logger.info("sender for #{name} is automatically closed by timeout.")
          @sender.shutdown
          @sender = nil
        end
      end
      @loop.attach(@auto_close_timer)
    end

    def log_tag
      "[#{Process.ppid}] engine-node: #{@name}"
    end
  end
end
