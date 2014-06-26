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
  module Test
    class StubHandlerMessenger
      attr_reader :values, :messages
      attr_accessor :engine_state

      def initialize
        @values = []
        @messages = []
        @engine_state = nil
      end

      def emit(value)
        @values << value
      end

      def forward(message, destination)
        @messages << [message, destination]
      end
    end
  end
end
