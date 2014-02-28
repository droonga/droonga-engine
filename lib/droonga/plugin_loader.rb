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

require "droonga/loggable"

module Droonga
  class PluginLoader
    include Loggable

    class << self
      def load_all
        loader = new
        loader.load_all
      end
    end

    def initialize
    end

    def load(name)
      require "droonga/plugins/#{name}"
    end

    def load_all
      loaded = []
      loading = nil
      begin
        $LOAD_PATH.each do |load_path|
          Pathname.glob("#{load_path}/droonga/plugins/*.rb") do |plugin_path|
            loading = plugin_path
            name = Pathname(plugin_path).basename(".rb").to_s
            load(name)
            loaded << plugin_path
          end
        end
      rescue StandardError, SyntaxError => error
        logger.info("loaded plugins:\n#{loaded.join("\n")}")
        logger.error("failed to load: #{loading}")
        raise error
      end
      logger.info("loaded plugins:\n#{loaded.join("\n")}")
    end

    private
    def log_tag
      "plugin-loader"
    end
  end
end
