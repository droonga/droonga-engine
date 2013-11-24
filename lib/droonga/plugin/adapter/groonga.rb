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

require "groonga"

require "droonga/adapter"

module Droonga
  class GroongaAdapter < Droonga::Adapter
    # TODO: AdapterPlugin or something should be defined to avoid conflicts.
    Droonga::LegacyPlugin.repository.register("select", self)
    command :select
    def select(select_request)
      command = Select.new
      search_request = command.convert_request(select_request)
      add_route("select_response")
      post(search_request, "search")
    end

    command :select_response
    def select_response(search_response)
      command = Select.new
      emit(command.convert_response(search_response))
    end
  end
end

require "droonga/plugin/adapter/groonga/select"
