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

require "droonga/handler_plugin"
require "droonga/command_mapper"
require "droonga/logger"

module Droonga
  class Handler
    class << self
      def inherited(sub_class)
        super
        sub_class.instance_variable_set(:@command_mapper, CommandMapper.new)
      end

      def command(name_or_map)
        @command_mapper.register(name_or_map)
      end

      def method_name(command)
        @command_mapper[command]
      end

      def handlable?(command)
        not method_name(command).nil?
      end
    end

    def initialize(worker)
      @worker = worker
      @context = @worker.context
    end

    def post(body, destination=nil)
      @worker.post(body, destination)
    end

    def envelope
      @worker.envelope
    end

    def add_route(route)
      @worker.add_route(route)
    end

    def shutdown
    end

    def handlable?(command)
      self.class.handlable?(command)
    end

    def handle(command, request, *arguments)
      __send__(self.class.method_name(command), request, *arguments)
    rescue => exception
      Logger.error("error while handling #{command}",
                   request: request,
                   arguments: arguments,
                   exception: exception)
    end

    def prefer_synchronous?(command)
      return false
    end
  end
end
