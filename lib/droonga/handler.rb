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

module Droonga
  class Handler
    class << self
      def inherited(sub_class)
        super
        sub_class.instance_variable_set(:@commands, {})
      end

      def command(name_or_map)
        if name_or_map.is_a?(Hash)
          command_map = name_or_map
          command_map.each do |command_name, method_name|
            @commands[command_name.to_s] = method_name
          end
        else
          name = name_or_map
          method_name = name
          @commands[name.to_s] = method_name
        end
      end

      def method_name(command)
        @commands[command.to_s]
      end

      def handlable?(command)
        not method_name(command).nil?
      end
    end

    def initialize(context)
      @context = context
    end

    def shutdown
    end

    def handlable?(command)
      self.class.handlable?(command)
    end

    def handle(command, request)
      __send__(self.class.method_name(command), request)
    end
  end
end
