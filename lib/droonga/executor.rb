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

require "msgpack"
require "fluent-logger"
require "groonga"

require "droonga/job_queue"
require "droonga/handler_plugin"
require "droonga/plugin"
require "droonga/catalog"
require "droonga/proxy"

module Droonga
  class Executor
    attr_reader :context, :envelope, :name

    def initialize(options={})
       @handlers = []
       @outputs = {}
       @name = options[:name]
       @database_name = options[:database] || "droonga/db"
       @queue_name = options[:queue_name] || "DroongaQueue"
       Droonga::JobQueue.ensure_schema(@database_name, @queue_name)
       @handler_names = options[:handlers] || ["proxy"]
       load_handlers
       @pool_size = options[:pool_size] || 1
       prepare
     end

    def add_handler(name)
      plugin = HandlerPlugin.new(name)
      @handlers << plugin.instantiate(self)
    end

    def add_route(route)
      envelope["via"].push(route)
    end

    def dispatch(*message)
      body, type, arguments = parse_message(message)
      post_or_push(message, body, "type" => type, "arguments" => arguments)
    end

    def post(body, destination=nil)
      post_or_push(nil, body, destination)
    end

    private
    def post_or_push(message, body, destination)
      route = nil
      unless destination
        route = envelope["via"].pop
        destination = route
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
      else
        receiver = envelope["replyTo"]
      end
      if receiver
        output(receiver, body, command, arguments)
      else
        handler = find_handler(command)
        if handler
          if synchronous.nil?
            synchronous = handler.prefer_synchronous?(command)
          end
          if route || @pool_size.zero? || synchronous
            handler.handle(command, body, *arguments)
          else
            unless message
              envelope["body"] = body
              envelope["type"] = command
              envelope["arguments"] = arguments
              message = ['', Time.now.to_f, envelope]
            end
            push_message(message)
          end
        end
      end
      add_route(route) if route
    end

    def output(receiver, body, command, arguments)
      return nil unless receiver
      unless receiver =~ /\A(.*):(\d+)\/(.*?)(\?.+)?\z/
        raise "format: hostname:port/tag(?params)"
      end
      host = $1
      port = $2
      tag  = $3
      params = $4
      output = get_output(host, port, params)
      return unless output
      if command
        message = envelope
        message["body"] = body
        message["type"] = command
        message["arguments"] = arguments
      else
        message = {
          inReplyTo: envelope["id"],
          statusCode: 200,
          type: (envelope["type"] || "") + ".result",
          body: body
        }
      end
      output.post(tag + ".message", message)
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

    def push_message(message)
      packed_message = message.to_msgpack
      queue = @context[@queue_name]
      queue.push do |record|
        record.message = packed_message
      end
    end

    def pull_message
      packed_message = nil
      @queue.pull do |record|
        if record
          packed_message = record.message
          record.delete
        end
      end
      return nil unless packed_message
      MessagePack.unpack(packed_message)
    end

    def load_handlers
      @handler_names.each do |handler_name|
        plugin = Droonga::Plugin.new("handler", handler_name)
        plugin.load
      end
    end

    def prepare
      @context = Groonga::Context.new
      @database = @context.open_database(@database_name)
      @context.encoding = :none
      @handler_names.each do |handler_name|
        add_handler(handler_name)
      end
      add_handler("proxy_message")
    end

    def find_handler(command)
      @handlers.find do |handler|
        handler.handlable?(command)
      end
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
  end
end
