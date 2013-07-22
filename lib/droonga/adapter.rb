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

require "droonga/command_mapper"

module Droonga
  class Adapter
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
    end

    def initialize(proxy)
      @proxy = proxy
    end

    def adapt(command, request)
      __send__(self.class.method_name(command), request)
    end

    def post(request, &block)
      @proxy.post(request, &block)
    end
  end
end
