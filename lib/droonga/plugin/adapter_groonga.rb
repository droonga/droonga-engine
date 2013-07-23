# Copyright (C) 2013 droonga project
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

require "groonga"

require "droonga/adapter"

module Droonga
  class GroongaAdapter < Droonga::Adapter
    command :select

    def select(select_request)
      command = Select.new
      search_request = command.select_convert_request(select_request)
      post(search_request) do |search_response|
        command.select_convert_response(search_response)
      end
      :selected
    end
  end
end

require "droonga/plugin/adapter_select"
