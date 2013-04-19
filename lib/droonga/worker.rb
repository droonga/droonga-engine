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

module Droonga
  class Worker
    attr_reader :context, :envelope

    def initialize(options={})
      @pool = []
      @handlers = []
      @outputs = {}
      @database_name = options[:database] || "droonga/db"
      @queue_name = options[:queue_name] || "DroongaQueue"
      Droonga::JobQueue.ensure_schema(@database_name, @queue_name)
      @handler_names = options[:handlers] || ["search"]
      load_handlers
      pool_size = options[:pool_size] || 1
      @pool = spawn(pool_size)
      prepare
    end

    def shutdown
      @pool.each do |pid|
        # TODO: do it gracefully
        Process.kill(:KILL, pid)
      end
      @handlers.each do |handler|
        handler.shutdown
      end
      @outputs.each do |dest, output|
        output[:logger].close if output[:logger]
      end
      @database.close
      @context.close
      @database = @context = nil
    end

    def dispatch(tag, time, record)
      if @pool.empty?
        process_message(tag, time, record)
      else
        post_message(tag, time, record)
      end
    end

    def add_handler(name)
      plugin = HandlerPlugin.new(name)
      @handlers << plugin.instantiate(self)
    end

    def add_route(route)
      envelope[:via].push(route)
    end

    def post(body, destination=nil)
      route = nil
      unless destination
        route = envelope[:via].pop
        destination = route
      end
      output = get_output(destination)
      return unless output
      if destination
        message = envelope
        message[:body] = body
      else
        message = {
          inReplyTo: envelope["id"],
          statusCode: 200,
          type: (envelope["type"] || "") + ".result",
          body: body
        }
      end
      output.post("message", message)
      add_route(route) if route
    end

    private
    def parse_message(tag, time, record)
      @message = [tag, time, record]
      prefix, type = tag.split(/\./)
      if type.nil? || type.empty? || type == 'message'
        @envelope = record
      else
        @envelope = {
          "type" => type,
          "body" => record
        }
      end
      envelope[:via] ||= []
    end

    def process_message(tag, time, record)
      parse_message(tag, time, record)
      command = envelope["type"]
      handler = find_handler(command)
      return unless handler
      handler.handle(command, envelope["body"])
    end

    def post_message(*message)
      packed_message = message.to_msgpack
      queue = @context[@queue_name]
      queue.push do |record|
        record.message = packed_message
      end
    end

    def start
      @finish = false
      @status = :IDLE
      # TODO: doesn't work
      Signal.trap(:TERM) do
        @finish = true
        exit! 0 if @status == :IDLE
      end
      queue = @context[@queue_name]
      while !@finish
        packed_message = nil
        queue.pull do |record|
          @status = :BUSY
          packed_message = record.message if record
        end
        if packed_message
          tag, time, record = MessagePack.unpack(packed_message)
          process_message(tag, time, record)
        end
        @status = :IDLE
      end
    end

    def spawn(pool_size)
      pool = []
      pool_size.times do
        pid = Process.fork
        if pid
          pool << pid
          next
        end
        # child process
        begin
          prepare
          start
          shutdown
          exit! 0
        end
      end
      pool
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
    end

    def find_handler(command)
      @handlers.find do |handler|
        handler.handlable?(command)
      end
    end

    def get_output(destination)
      receiver = envelope["replyTo"]
      return nil unless receiver
      unless receiver =~ /\A(.*):(\d+)\/(.*?)(\?.+)?\z/
        raise "format: hostname:port/tag(?params)"
      end
      host = $1
      port = $2
      tag  = $3
      params = $4

      host_port = "#{host}:#{port}"
      @outputs[host_port] ||= {}
      output = @outputs[host_port]

      has_connection_id = (not params.nil? \
                           and params =~ /[\?&;]connection_id=([^&;]+)/)
      if output[:logger].nil? or has_connection_id
        connection_id = $1
        if not has_connection_id or output[:connection_id] != connection_id
          output[:connection_id] = connection_id
          logger = create_logger(tag, :host => host, :port => port.to_i)
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

    def create_logger(tag, options)
      Fluent::Logger::FluentLogger.new(tag, options)
    end
  end
end
