# -*- coding: utf-8 -*-
#
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

require "pathname"

module Droonga
  class PluginLoader
    class << self
      def load_all
        loaded = []
        loading = nil
        begin
          $LOAD_PATH.each do |load_path|
            Pathname.glob("#{load_path}/droonga/plugins/*.rb") do |plugin_path|
              loading = plugin_path
              name = Pathname(plugin_path).basename(".rb").to_s
              loader = new(name)
              loader.load
              loaded << plugin_path
            end
          end
        rescue StandardError, SyntaxError => error
          $log.info("#{self.name}: loaded plugins:\n#{loaded.join("\n")}")
          $log.error("#{self.name}: failed to load: #{loading}")
          raise error
        end
        $log.info("#{self.name}: loaded plugins:\n#{loaded.join("\n")}")
      end
    end

    def initialize(name)
      @name = name
    end

    def load
      require "droonga/plugins/#{@name}"
    end
  end
end
