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

require "droonga/pluggable"
require "droonga/loggable"
require "droonga/plugin/metadata/input_message"
require "droonga/plugin/metadata/handler_action"
require "droonga/error_messages"

module Droonga
  class Handler
    extend Pluggable
    include Loggable
    include ErrorMessages

    class << self
      def message
        Plugin::Metadata::InputMessage.new(self)
      end

      def action
        Plugin::Metadata::HandlerAction.new(self)
      end
    end

    attr_reader :label, :messenger, :loop
    def initialize(name, label, context, messenger, loop)
      @name = name
      @label = label
      @context = context
      @messenger = messenger
      @loop = loop
    end

    def handle(message)
    end
  end
end
