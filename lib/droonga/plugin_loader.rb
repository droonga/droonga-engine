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
        $LOAD_PATH.each do |load_path|
          Dir.glob("#{load_path}/droonga/plugin/*") do |type_path|
            next unless File.directory?(type_path)
            type = File.basename(type_path)
            Dir.glob("#{type_path}/*.rb") do |path|
              $log.info("#{self.name}: loading: #{path}")
              name = File.basename(path, ".rb")
              loader = new(type, name)
              loader.load
            end
          end

          Pathname.glob("#{load_path}/droonga/plugins/*.rb") do |plugin_path|
            $log.info("#{self.name}: loading: #{plugin_path}")
            relative_plugin_path =
              plugin_path.relative_path_from(Pathname(load_path))
            require_path = relative_plugin_path.to_s.gsub(/\.rb\z/, "")
            require require_path
          end
        end
      end
    end

    def initialize(type, name)
      @type = type
      @name = name
    end

    def load
      return if @type == "metadata"
      require "droonga/plugin/#{@type}/#{@name}"
    end
  end
end
