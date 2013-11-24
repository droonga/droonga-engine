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
require "droonga/pluggable"
require "droonga/handler_plugin"

module Droonga
  class Handler
    include Pluggable

    attr_reader :context, :envelope, :name

    def initialize(options={})
      @outputs = {}
      @options = options
      @name = options[:name]
      @database_name = options[:database]
      @queue_name = options[:queue_name] || "DroongaQueue"
      prepare
    end

    def shutdown
      $log.trace("#{log_tag}: shutdown: start")
      super
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

    def execute_one
      $log.trace("#{log_tag}: execute_one: start")
      message = @job_queue.pull_message
      unless message
        $log.trace("#{log_tag}: execute_one: abort: no message")
        return
      end
      process(message)
      $log.trace("#{log_tag}: execute_one: done")
    end

    def prefer_synchronous?(command)
      find_plugin(command).prefer_synchronous?(command)
    end

    def process(message)
      $log.trace("#{log_tag}: process: start")
      body, command, arguments = parse_message(message)
      plugin = find_plugin(command)
      if plugin.nil?
        $log.trace("#{log_tag}: process: done: no plugin: <#{command}>")
        return
      end
      process_command(plugin, command, body, arguments)
      $log.trace("#{log_tag}: process: done: <#{command}>",
                 :plugin => plugin.class)
    end

    def emit(value, name = nil)
      unless name
        if @output_names
          name = @output_names.first
        else
          @output_values = @task["values"] = value
          return
        end
      end
      @output_values[name] = value
    end

    def post(body, destination)
      $log.trace("#{log_tag}: post: start")
      command = destination["type"]
      receiver = destination["to"]
      arguments = destination["arguments"]
      synchronous = destination["synchronous"]
      output(receiver, body, command, arguments)
      $log.trace("#{log_tag}: post: done")
    end

    def handle(command, body, synchronous=nil)
      plugin = find_plugin(command)
      if synchronous.nil?
        synchronous = plugin.prefer_synchronous?(command)
      end
      if synchronous
        $log.trace("#{log_tag}: post_or_push: process: start")
        plugin.process(command, body, *arguments)
        $log.trace("#{log_tag}: post_or_push: process: done")
      else
        envelope["body"] = body
        envelope["type"] = command
        envelope["arguments"] = arguments
        message = ['', Time.now.to_f, envelope]
        @job_queue.push_message(message)
      end
    end

    private
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

    def prepare
      if @database_name && !@database_name.empty?
        @context = Groonga::Context.new
        @database = @context.open_database(@database_name)
        @job_queue = JobQueue.open(@database_name, @queue_name)
      end
      load_plugins(@options[:handlers] || [])
    end

    def instantiate_plugin(name)
      HandlerPlugin.repository.instantiate(name, self)
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

    def process_command(plugin, command, request, arguments)
      return false unless request.is_a? Hash

      @task = request["task"]
      return false unless @task.is_a? Hash

      @component = @task["component"]
      return false unless @component.is_a? Hash

      @output_values = @task["values"]
      @body = @component["body"]
      @output_names = @component["outputs"]
      @id = request["id"]
      @value = request["value"]
      @input_name = request["name"]
      @descendants = request["descendants"]

      plugin.process(command, @body, *arguments)
      output_xxx if @descendants
      true
    end

    def output_xxx
      result = @task["values"]
      if @component["post"]
        destination = @component["post"]
        destination = envelope["replyTo"] if destination == true
        post(result, destination)
      end
      @descendants.each do |name, dests|
        message = {
          "id" => @id,
          "input" => name,
          "value" => result[name]
        }
        dests.each do |routes|
          routes.each do |route|
            post(message, "to"=>route, "type"=>"dispatcher")
          end
        end
      end
    end

    def create_logger(options)
      Fluent::Logger::FluentLogger.new(nil, options)
    end

    def log_tag
      "[#{Process.ppid}][#{Process.pid}] handler"
    end
  end
end
