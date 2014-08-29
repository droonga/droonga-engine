# Copyright (C) 2014 Droonga Project
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "pathname"

module Droonga
  module Path
    BASE_DIR_ENV_NAME = "DROONGA_BASE_DIR"

    class << self
      def setup
        base_dir = ENV[BASE_DIR_ENV_NAME] || Dir.pwd
        ENV[BASE_DIR_ENV_NAME] = File.expand_path(base_dir)
      end

      def base
        @base ||= Pathname.new(ENV[BASE_DIR_ENV_NAME] || Dir.pwd).expand_path
      end

      def base=(new_base)
        @base = nil
        ENV[BASE_DIR_ENV_NAME] = new_base
      end

      def state
        base + "state"
      end

      def live_nodes
        state + "live-nodes.json"
      end

      def last_processed_timestamp
        state + "last-processed.timestamp"
      end

      def effective_message_timestamp
        state + "effective-message.timestamp"
      end

      def config
        base + "droonga-engine.yaml"
      end

      def default_pid_file
        base + "droonga-engine.pid"
      end

      def default_log_file
        base + "droonga-engine.log"
      end

      def catalog
        base_file_name = ENV["DROONGA_CATALOG"] || "catalog.json"
        Pathname.new(base_file_name).expand_path(base)
      end

      def published(suffix)
        base + "published-#{suffix}"
      end

      def buffer
        state + "buffer"
      end
    end
  end
end
