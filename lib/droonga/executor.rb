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
require "groonga"

require "droonga/job_queue"
require "droonga/handler_plugin"
require "droonga/plugin_loader"
require "droonga/dispatcher"

module Droonga
  class Executor
    attr_reader :context, :envelope, :name

    def initialize(options={})
      @handlers = []
      @outputs = {}
      @options = options
      @name = options[:name]
      @database_name = options[:database]
      @queue_name = options[:queue_name] || "DroongaQueue"
      @handler_names = options[:handlers] || []
      @pool_size = options[:n_workers] || 0
#     load_handlers
      Droonga::PluginLoader.load_all
      prepare
    end

    def shutdown
      $log.trace("#{log_tag}: shutdown: start")
      @handlers.each do |handler|
        handler.shutdown
      end
      @outputs.each do |dest, output|
        output[:logger].close if output[:logger]
      end
      if @database
        @database.close
        @context.close
        @database = @context = nil
      end
      if @job_queue
        @job_queue.close
        @job_queue = nil
      end
      $log.trace("#{log_tag}: shutdown: done")
    end

    def add_handler(name)
      handler = LegacyPlugin.repository.instantiate(name, self)
      @handlers << handler
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

    def execute_one
      $log.trace("#{log_tag}: execute_one: start")
      message = @job_queue.pull_message
      unless message
        $log.trace("#{log_tag}: execute_one: abort: no message")
        return
      end
      body, command, arguments = parse_message(message)
      handler = find_handler(command)
      if handler
        $log.trace("#{log_tag}: execute_one: handle: start",
                   :hander => handler.class)
        handler.handle(command, body, *arguments)
        $log.trace("#{log_tag}: execute_one: handle: done",
                   :hander => handler.class)
      end
      $log.trace("#{log_tag}: execute_one: done")
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
        output(receiver, body, command, arguments)
      else
        handler = find_handler(command)
        if handler
          if synchronous.nil?
            synchronous = handler.prefer_synchronous?(command)
          end
          if route || @pool_size.zero? || synchronous
            $log.trace("#{log_tag}: post_or_push: handle: start")
            handler.handle(command, body, *arguments)
            $log.trace("#{log_tag}: post_or_push: handle: done")
          else
            unless message
              envelope["body"] = body
              envelope["type"] = command
              envelope["arguments"] = arguments
              message = ['', Time.now.to_f, envelope]
            end
            @job_queue.push_message(message)
          end
        end
      end
      add_route(route) if route
    end

    def is_route?(route)
      route.is_a?(String) || route.is_a?(Hash)
    end

    def output(receiver, body, command, arguments)
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

    def load_handlers
      @handler_names.each do |handler_name|
        loader = Droonga::PluginLoader.new("handler", handler_name)
        loader.load
      end
    end

    def prepare
      if @database_name && !@database_name.empty?
        @context = Groonga::Context.new
        @database = @context.open_database(@database_name)
        @job_queue = JobQueue.open(@database_name, @queue_name)
      end
      @handler_names.each do |handler_name|
        add_handler(handler_name)
      end
      add_handler("dispatcher_message") unless @options[:standalone]
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

    def log_tag
      "[#{Process.ppid}][#{Process.pid}] executor"
    end
  end
end
