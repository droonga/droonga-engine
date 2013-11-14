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

module Droonga
  class CommandMapper
    def initialize
      @commands = {}
    end

    def register(name_or_map)
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

    def [](command)
      @commands[command.to_s]
    end

    def commands
      @commands.keys
    end
  end
end
