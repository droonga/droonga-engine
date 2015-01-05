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

require "droonga/loggable"

module Droonga
  class Serf
    class Command
      include Loggable

      def initialize(serf, command, *options)
        @serf = serf
        @command = command
        @options = options
      end

      def run
        stdout, stderror, status = Open3.capture3(@serf, @command, *@options, :pgroup => true)
        {
          :result => stdout,
          :error  => stderror,
          :status => status,
        }
      end

      def log_tag
        "serf[#{@command}]"
      end
    end
  end
end
