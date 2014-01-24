# Copyright (C) 2013-2014 Droonga Project
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

module Droonga
  class GroongaInputAdapter < Droonga::InputAdapterPlugin
    repository.register("groonga", self)

    command :select
    def select(input_message)
      command = Select.new
      select_request = input_message.body
      search_request = command.convert(select_request)
      input_message.add_route("select_response")
      input_message.command = "search"
      input_message.body = search_request
    end

    command :table_create
    def table_create(input_message)
      input_message.add_route("groonga_generic_response")
    end

    command :table_remove
    def table_remove(input_message)
      input_message.add_route("groonga_generic_response")
    end

    command :column_create
    def column_create(input_message)
      input_message.add_route("groonga_generic_response")
    end
  end
end

require "droonga/plugin/input_adapter/groonga/select"
