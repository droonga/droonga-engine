# Copyright (C) 2013-2014 Droonga Project
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
require "droonga/handler_message"
require "droonga/handler_messenger"
require "droonga/handler"

module Droonga
  class HandlerRunner
    class HandlerError < Error
    end

    class MissingMessageType < HandlerError
      def initialize(handler_classes, dataset_name)
        message = "[#{dataset_name}] \"message.type\" is not specified for " +
                    "handler class(es): <#{handler_classes.inspect}>"
        super(message)
      end
    end

    class ConflictForSameType < HandlerError
      def initialize(types, dataset_name)
        message = "[#{dataset_name}] There are conflicting handlers for " +
                    "same message type: <#{types.inspect}>"
        super(message)
      end
    end

    def initialize(loop, options={})
      @loop = loop
      @options = options
      @name = options[:name]
      @dataset_name = options[:dataset]
      @database_name = options[:database]
      prepare
    end

    def start
      $log.trace("#{log_tag}: start: start")
      @forwarder.start
      $log.trace("#{log_tag}: start: done")
    end

    def shutdown
      $log.trace("#{log_tag}: shutdown: start")
      @forwarder.shutdown
      if @database
        @database.close
        @context.close
        @database = @context = nil
      end
      $log.trace("#{log_tag}: shutdown: done")
    end

    def prefer_synchronous?(type)
      find_handler_class(type).action.synchronous?
    end

    def processable?(type)
      not find_handler_class(type).nil?
    end

    def process(message)
      $log.trace("#{log_tag}: process: start")
      type = message["type"]
      handler_class = find_handler_class(type)
      if handler_class.nil?
        $log.trace("#{log_tag}: process: done: no handler: <#{type}>")
        return
      end
      process_type(handler_class, type, message)
      $log.trace("#{log_tag}: process: done: <#{type}>",
                 :handler => handler_class)
    end

    private
    def prepare
      if @database_name and !@database_name.empty?
        @context = Groonga::Context.new
        @database = @context.open_database(@database_name)
      end
      $log.debug("#{self.class.name}: activating plugins for the dataset \"#{@dataset_name}\": " +
                   "#{@options[:plugins].join(", ")}")
      @handler_classes = Handler.find_sub_classes(@options[:plugins] || [])
      validate_handler_classes
      $log.debug("#{self.class.name}: activated:\n#{@handler_classes.join("\n")}")
      @forwarder = Forwarder.new(@loop)
    end

    def find_handler_class(type)
      @handler_classes.find do |handler_class|
        handler_class.message.type == type
      end
    end

    def validate_handler_classes
      types = {}
      missing_type_handlers = []

      @handler_classes.each do |handler_class|
        type = handler_class.message.type
        if type.nil? or type.empty?
          missing_type_handlers << handler_class
          next
        end
        types[type] ||= []
        types[type] << handler_class
      end

      if missing_type_handlers.size > 0
        raise MissingMessageType.new(missing_type_handlers, @dataset_name)
      end

      types.each do |type, handler_classes|
        types.delete(type) if handler_classes.size == 1
      end
      if types.size > 0
        raise ConflictForSameType.new(types, @dataset_name)
      end
    end

    def process_type(handler_class, type, raw_message)
      handler_message = HandlerMessage.new(raw_message)
      handler_message.validate

      messenger = HandlerMessenger.new(@forwarder, handler_message, @options)
      handler = handler_class.new(@name, @context)
      begin
        handler.handle(handler_message, messenger)
      rescue ErrorMessage => error
        messenger.error(error.status_code, error.response_body)
      end
    end

    def log_tag
      "[#{Process.ppid}][#{Process.pid}] handler"
    end
  end
end
