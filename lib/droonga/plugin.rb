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

module Droonga
  class Plugin
    class << self
      def load_all
        $LOAD_PATH.each do |load_path|
          Dir.glob("#{load_path}/droonga/plugin/*_*.rb") do |path|
            type, name = File.basename(path, ".rb").split(/_/, 2)
            plugin = new(type, name)
            plugin.load
          end
        end
      end
    end

    def initialize(type, name)
      @type = type
      @name = name
    end

    def load
      require "droonga/plugin/#{@type}/#{@type}_#{@name}"
    end
  end
end
