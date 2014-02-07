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
require "droonga/legacy_pluggable"
require "droonga/handler_plugin"

module Droonga
  class HandlerRunner
    include LegacyPluggable

    attr_reader :context, :name

    def initialize(loop, options={})
      @loop = loop
      @options = options
      @name = options[:name]
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

    def process(message)
      $log.trace("#{log_tag}: process: start")
      command = message["type"]
      plugin = find_plugin(command)
      if plugin.nil?
        $log.trace("#{log_tag}: process: done: no plugin: <#{command}>")
        return
      end
      process_command(plugin, command, message)
      $log.trace("#{log_tag}: process: done: <#{command}>",
                 :plugin => plugin.class)
    end

    private
    def prepare
      if @database_name && !@database_name.empty?
        @context = Groonga::Context.new
        @database = @context.open_database(@database_name)
      end
      load_plugins(@options[:handlers] || [])
      @forwarder = Forwarder.new(@loop)
    end

    def instantiate_plugin(name)
      HandlerPlugin.repository.instantiate(name, self)
    end

    def process_command(plugin, command, raw_message)
      handler_message = HandlerMessage.new(raw_message)
      handler_message.validate

      messenger = HandlerMessenger.new(@forwarder, handler_message, @options)
      plugin.process(command, handler_message, messenger)
    end

    def log_tag
      "[#{Process.ppid}][#{Process.pid}] handler"
    end
  end
end
