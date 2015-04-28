# Copyright (C) 2014-2015 Droonga Project
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

      def databases(base_path=nil)
        base_path ||= base
        path = Pathname(base_path) + "databases"
        path.expand_path
      end

      def state
        base + "state"
      end

      def cluster_state
        state + "cluster-state.json"
      end

      def config
        base + "droonga-engine.yaml"
      end

      def default_log_file
        base + "droonga-engine.log"
      end

      def catalog
        base_file_name = ENV["DROONGA_CATALOG"] || "catalog.json"
        Pathname.new(base_file_name).expand_path(base)
      end

      def last_message_timestamp
        base + "last-message-timestamp.txt"
      end

      def accidental_buffer
        state + "buffer" + "accidental"
      end

      def intentional_buffer
        state + "buffer" + "intentional"
      end

      def serf_command
        base + "serf"
      end

      def serf_tags_file
        state + "serf-tags.json"
      end

      def serf_event_handler_errors
        state + "serf-event-handler-errors"
      end

      def serf_event_handler_error_file
        now = Time.now
        name = sprintf("%04d-%02d-%02d_%02d-%02d-%02d.%d.error",
                       now.year, now.month, now.day,
                       now.hour, now.min, now.sec, now.nsec)
        serf_event_handler_errors + name
      end

      def unique_file_path(directory, basename, suffix)
        directory = Pathname(directory)
        basename = basename.sub(/\.\z/, "")
        suffix   = suffix.sub(/\A\./, "")
        uniqueness_count = 0
        path = nil
        begin
          path = directory + "#{basename}.#{uniqueness_count}.#{suffix}"
          uniqueness_count += 1
        end while path.exist?
        path
      end
    end
  end
end
