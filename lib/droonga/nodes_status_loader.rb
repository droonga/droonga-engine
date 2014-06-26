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
  class NodesStatusLoader
    def initialize(path)
      @path = path
    end

    def load
      return default_status unless @path.exist?

      contents = @path.read
      return default_status if contents.empty?

      begin
        JSON.parse(contents)
      rescue JSON::ParserError
        default_status
      end
    end

    private
    def default_status
      {}
    end
  end
end
