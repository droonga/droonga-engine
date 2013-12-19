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

require "droonga/input_adapter_plugin"
require "droonga/output_adapter_plugin"

module Droonga
  class GroongaInputAdapter < Droonga::InputAdapterPlugin
    repository.register("select", self)

    command :select
    def select(input_message)
      command = GroongaAdapter::Select.new
      select_request = input_message.body
      search_request = command.convert_request(select_request)
      input_message.add_route("select_response")
      input_message.command = "search"
      input_message.body = search_request
    end
  end

  class GroongaOutputAdapter < Droonga::OutputAdapterPlugin
    repository.register("select", self)

    command :select_response
    def select_response(search_response)
      command = GroongaAdapter::Select.new
      emit(command.convert_response(search_response))
    end
  end
end

require "droonga/plugin/adapter/groonga/select"
