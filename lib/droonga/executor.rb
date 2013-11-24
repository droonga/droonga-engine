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

require "groonga"

require "droonga/forwarder"
require "droonga/dispatcher"
require "droonga/distributor"

module Droonga
  class Executor
    attr_reader :context, :envelope, :name

    def initialize(options={})
      @options = options
      @name = options[:name]
      @database_name = options[:database]
      @queue_name = options[:queue_name] || "DroongaQueue"
      @pool_size = options[:n_workers] || 0
      prepare
    end

    def shutdown
      $log.trace("#{log_tag}: shutdown: start")
      @distributor.shutdown
      @forwarder.shutdown
      if @database
        @database.close
        @context.close
        @database = @context = nil
      end
      $log.trace("#{log_tag}: shutdown: done")
    end

    def add_route(route)
      envelope["via"].push(route)
    end

    def dispatch(tag, time, record, synchronous=nil)
      $log.trace("#{log_tag}: dispatch: start")
      message = [tag, time, record]
      body, type, arguments = parse_message([tag, time, record])
      reply_to = envelope["replyTo"]
      if reply_to.is_a? String
        envelope["replyTo"] = {
          "type" => type + ".result",
          "to" => reply_to
        }
      end
      post_or_push(message, body,
                   "type" => type,
                   "arguments" => arguments,
                   "synchronous" => synchronous)
      $log.trace("#{log_tag}: dispatch: done")
    end

    def post(body, destination=nil)
      $log.trace("#{log_tag}: post: start")
      post_or_push(nil, body, destination)
      $log.trace("#{log_tag}: post: done")
    end

    private
    def post_or_push(message, body, destination)
      route = nil
      unless is_route?(destination)
        route = envelope["via"].pop
        destination = route
      end
      unless is_route?(destination)
        destination = envelope["replyTo"]
      end
      command = nil
      receiver = nil
      arguments = nil
      synchronous = nil
      case destination
      when String
        command = destination
      when Hash
        command = destination["type"]
        receiver = destination["to"]
        arguments = destination["arguments"]
        synchronous = destination["synchronous"]
      end
      if receiver
        @forwarder.forward(envelope, body,
                           "type" => command,
                           "to" => receiver,
                           "arguments" => arguments)
      else
        if command == "dispatcher"
          @dispatcher.handle(body, arguments)
        elsif @dispatcher.processable?(command)
          @dispatcher.process(command, body, *arguments)
        else
          @distributor.distribute(envelope.merge("type" => command,
                                                 "body" => body))
        end
      end
      add_route(route) if route
    end

    def is_route?(route)
      route.is_a?(String) || route.is_a?(Hash)
    end

    def parse_message(message)
      tag, time, record = message
      prefix, type, *arguments = tag.split(/\./)
      if type.nil? || type.empty? || type == 'message'
        @envelope = record
      else
        @envelope = {
          "type" => type,
          "arguments" => arguments,
          "body" => record
        }
      end
      envelope["via"] ||= []
      [envelope["body"], envelope["type"], envelope["arguments"]]
    end

    def prepare
      if @database_name && !@database_name.empty?
        @context = Groonga::Context.new
        @database = @context.open_database(@database_name)
      end
      @dispatcher = Dispatcher.new(self, name)
      @distributor = Distributor.new(@dispatcher, @options)
      @forwarder = Forwarder.new
    end

    def log_tag
      "[#{Process.ppid}][#{Process.pid}] executor"
    end
  end
end
