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

require "pathname"
require "json"

require "droonga/live_nodes_list"

module Droonga
  class LiveNodesListLoader
    def initialize(path)
      @path = path
    end

    def load
      list = parse
      LiveNodesList.new(list)
    end

    private
    def parse
      return default_list unless @path.exist?

      contents = @path.read
      return default_list if contents.empty?

      begin
        JSON.parse(contents)
      rescue JSON::ParserError
        default_list
      end
    end

    def default_list
      {}
    end
  end
end
