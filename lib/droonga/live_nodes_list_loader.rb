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

require "pathname"
require "json"

module Droonga
  class LiveNodesListLoader
    def initialize(file_path)
      @file_path = file_path
    end

    def load
      list_file = Pathname(@file_path)
      list = parse_list_file(list_file)
      list.keys
    end

    private
    def parse_list_file(list_file)
      return default_list unless list_file
      return default_list unless list_file.exist?

      contents = list_file.read
      return default_list if contents.empty?

      begin
        JSON.parse(contents).keys
      rescue JSON::ParserError
        default_list
      end
    end

    def default_list
      {}
    end
  end
end
