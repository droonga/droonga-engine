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

    def prefer_synchronous?(command)
      find_handler_class(command).action.synchronous?
    end

    def processable?(command)
      not find_handler_class(command).nil?
    end

    def process(message)
      $log.trace("#{log_tag}: process: start")
      command = message["type"]
      handler_class = find_handler_class(command)
      if handler_class.nil?
        $log.trace("#{log_tag}: process: done: no handler: <#{command}>")
        return
      end
      process_command(handler_class, command, message)
      $log.trace("#{log_tag}: process: done: <#{command}>",
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
      $log.debug("#{self.class.name}: activated:\n#{@handler_classes.join("\n")}")
      @forwarder = Forwarder.new(@loop)
    end

    def find_handler_class(command)
      @handler_classes.find do |handler_class|
        handler_class.message.type == command
      end
    end

    def process_command(handler_class, command, raw_message)
      handler_message = HandlerMessage.new(raw_message)
      handler_message.validate

      messenger = HandlerMessenger.new(@forwarder, handler_message, @options)
      handler = handler_class.new(@name, @context)
      begin
        handler.handle(handler_message, messenger)
      rescue MessageProcessingError => error
        messenger.error(error.status_code, error.response_body)
      end
    end

    def log_tag
      "[#{Process.ppid}][#{Process.pid}] handler"
    end
  end
end
