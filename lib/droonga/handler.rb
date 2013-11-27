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

require "droonga/job_queue"
require "droonga/forwarder"
require "droonga/pluggable"
require "droonga/handler_plugin"

module Droonga
  class Handler
    include Pluggable

    attr_reader :context, :envelope, :name

    def initialize(options={})
      @options = options
      @name = options[:name]
      @database_name = options[:database]
      prepare
    end

    def shutdown
      $log.trace("#{log_tag}: shutdown: start")
      super
      @forwarder.shutdown
      if @database
        @database.close
        @context.close
        @database = @context = nil
      end
      $log.trace("#{log_tag}: shutdown: done")
    end

    def prefer_synchronous?(command)
      find_plugin(command).prefer_synchronous?(command)
    end

    def process(envelope)
      $log.trace("#{log_tag}: process: start")
      body, command, arguments = parse_envelope(envelope)
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

    def post(message, destination)
      @forwarder.forward(envelope, message, destination)
    end

    private
    def parse_envelope(envelope)
      @envelope = envelope
      envelope["via"] ||= []
      [envelope["body"], envelope["type"], envelope["arguments"]]
    end

    def prepare
      if @database_name && !@database_name.empty?
        @context = Groonga::Context.new
        @database = @context.open_database(@database_name)
      end
      load_plugins(@options[:handlers] || [])
      @forwarder = Forwarder.new
    end

    def instantiate_plugin(name)
      HandlerPlugin.repository.instantiate(name, self)
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
      output if @descendants
      true
    end

    def output
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

    def log_tag
      "[#{Process.ppid}][#{Process.pid}] handler"
    end
  end
end
