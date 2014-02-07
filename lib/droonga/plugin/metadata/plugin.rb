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
  module Plugin
    module Metadata
      class Plugin
        def initialize(plugin_class)
          @plugin_class = plugin_class
        end

        def name
          configuration[:name]
        end

        def name=(name)
          configuration[:name] = name
        end

        private
        def configuration
          @plugin_class.options[:plugin] ||= {}
        end
      end
    end
  end
end
