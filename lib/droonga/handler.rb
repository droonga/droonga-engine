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
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

require "droonga/pluggable"
require "droonga/plugin/metadata/input_message"
require "droonga/plugin/metadata/handler_action"

module Droonga
  class Handler
    extend Pluggable

    class << self
      def message
        Plugin::Metadata::InputMessage.new(self)
      end

      def action
        Plugin::Metadata::HandlerAction.new(self)
      end
    end

    def initialize(name, context)
      @name = name
      @context = context
    end

    def handle(message, messenger)
    end
  end
end
