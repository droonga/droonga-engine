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

require "droonga/output_adapter_plugin"

module Droonga
  class GroongaOutputAdapter < Droonga::OutputAdapterPlugin
    repository.register("groonga", self)

    command :select_response
    def select_response(output_message)
      command = Select.new
      output_message.body = command.convert(output_message.body)
    end
  end
end

require "droonga/plugin/output_adapter/groonga/select"
