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

require "open3"
require "pp"

require "droonga/loggable"

module Droonga
  class Serf
    class Command
      class Failure < Error
        attr_reader :command_line, :exit_status, :output, :error
        attr_accessor :verbose
        def initialize(command_line, exit_status, output, error)
          @command_line = command_line
          @exit_status = exit_status
          @output = output
          @error = error
          @verbose = false
          message = "Failed to run serf: (#{@exit_status}): "
          message << "#{@error.strip}[#{@output.strip}]: "
          message << @command_line.join(" ")
          super(message)
        end
      end

      include Loggable

      def initialize(serf, command, *options)
        @serf = serf
        @command = command
        @options = options
      end

      def run
        command_line = [@serf, @command] + @options
        p command_line if @verbose
        stdout, stderror, status = Open3.capture3(*command_line,
                                                  :pgroup => true)
        unless status.success?
          raise Failure.new(command_line, status.to_i, stdout, stderror)
        end
        logger.error("run: #{stderror}") unless stderror.empty?
        if @verbose
          begin
            pp JSON.parse(stdout)
          rescue JSON::ParserError
            p stdout
          end
        end
        stdout
      end

      def log_tag
        "serf[#{@command}]"
      end
    end
  end
end
