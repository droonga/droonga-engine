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
require "droonga/plugin/metadata/adapter_input_message"
require "droonga/plugin/metadata/adapter_output_message"
require "droonga/error_messages"

module Droonga
  class Adapter
    extend Pluggable
    include ErrorMessages

    class << self
      def input_message
        Plugin::Metadata::AdapterInputMessage.new(self)
      end

      def output_message
        Plugin::Metadata::AdapterOutputMessage.new(self)
      end

      def id
        options[:id] || name || object_id.to_s
      end
    end

    def adapt_input(input_message)
    end

    def adapt_output(output_message)
    end
  end
end
