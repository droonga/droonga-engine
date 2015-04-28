# Copyright (C) 2015 Droonga Project
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

require "time"

require "droonga/path"
require "droonga/safe_file_writer"
require "droonga/file_observer"

module Droonga
  module Timestamp
    MICRO_SECONDS_DECIMAL_PLACE = 6

    class << self
      def stringify(timestamp)
        timestamp.utc.iso8601(MICRO_SECONDS_DECIMAL_PLACE)
      end

      def last_message_timestamp=(timestamp)
        if timestamp.is_a?(String)
          if timestamp.empty?
            timestamp = nil
          else
            timestamp = Time.parse(timestamp)
          end
        end
        if timestamp
          timestamp = stringify(timestamp)
        else
          timestamp = ""
        end
        SafeFileWriter.write(Path.last_message_timestamp) do |output, file|
          output.puts(timestamp)
        end
      end

      def last_message_timestamp
        file = Path.last_message_timestamp
        return nil unless file.exist?
        timestamp = file.read
        return nil if timestamp.nil? or timestamp.empty?
        Time.parse(timestamp)
      end

      def run_last_message_timestamp_observer(loop, &block)
        path = Path.last_message_timestamp
        observer = FileObserver.new(loop, path)
        observer.on_change = lambda do
          yield(last_message_timestamp)
        end
        observer.start
        observer
      end
    end
  end
end
