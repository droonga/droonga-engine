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

require "droonga/loggable"
require "droonga/forwarder"
require "droonga/handler_message"
require "droonga/handler_messenger"
require "droonga/step_runner"

module Droonga
  class HandlerRunner
    include Loggable

    def initialize(loop, options={})
      @loop = loop
      @options = options
      @name = options[:name]
      @dataset_name = options[:dataset]
      @database_name = options[:database]
      prepare
    end

    def start
      logger.trace("start: start")
      @forwarder.start
      logger.trace("start: done")
    end

    def shutdown
      logger.trace("shutdown: start")
      @forwarder.shutdown
      if @database
        @database.close
        @context.close
        @database = @context = nil
      end
      logger.trace("shutdown: done")
    end

    def prefer_synchronous?(type)
      find_handler_class(type).action.synchronous?
    end

    def processable?(type)
      not find_handler_class(type).nil?
    end

    def process(message)
      logger.trace("process: start")
      type = message["type"]
      handler_class = find_handler_class(type)
      if handler_class.nil?
        logger.trace("process: done: no handler: <#{type}>")
        return
      end
      process_type(handler_class, type, message)
      logger.trace("process: done: <#{type}>",
                   :handler => handler_class)
    end

    private
    def prepare
      if @database_name and !@database_name.empty?
        @context = Groonga::Context.new
        @database = @context.open_database(@database_name)
      end
      logger.debug("#{self.class.name}: activating plugins for the dataset \"#{@dataset_name}\": " +
                     "#{@options[:plugins].join(", ")}")
      @step_runner = StepRunner.new(nil, @options[:plugins] || [])
      @forwarder = Forwarder.new(@loop)
    end

    def find_handler_class(type)
      step_definition = @step_runner.find(type)
      return nil if step_definition.nil?
      step_definition.handler_class
    end

    def process_type(handler_class, type, raw_message)
      handler_message = HandlerMessage.new(raw_message)
      handler_message.validate

      messenger = HandlerMessenger.new(@forwarder, handler_message, @options)
      handler = handler_class.new(@name, @context, messenger, @loop)
      begin
        result = handler.handle(handler_message)
        unless result.nil?
          # XXX: It is just a workaround.
          # Remove me when super step is introduced.
          if handler.is_a?(Droonga::Plugins::Search::Handler)
            messenger.emit(result)
          else
            messenger.emit("result" => result)
          end
        end
      rescue ErrorMessage => error
        messenger.error(error.status_code, error.response_body)
      rescue => error
        logger.exception("failed to handle message", error)
        internal_server_error =
          ErrorMessages::InternalServerError.new("Unknown internal error")
        messenger.error(internal_server_error.status_code,
                        internal_server_error.response_body)
      end
    end

    def log_tag
      "[#{Process.ppid}][#{Process.pid}] handler"
    end
  end
end
