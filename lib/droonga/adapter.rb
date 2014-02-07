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

module Droonga
  class Adapter
    class << self
      def sub_classes
        @@sub_classes ||= []
      end

      def inherited(sub_class)
        super
        sub_classes << sub_class
      end

      def plugin
        PluginConfiguration.new(self)
      end

      def message
        MessageConfiguration.new(self)
      end

      def id
        options[:id] || name || object_id.to_s
      end

      def options
        @options ||= {}
      end
    end

    def adapt_input(input_message)
    end

    def adapt_output(output_message)
    end
  end
end

require "droonga/adapter/plugin_configuration"
require "droonga/adapter/message_configuration"
