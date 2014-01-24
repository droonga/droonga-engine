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

require "droonga/output_adapter_plugin"

module Droonga
  class CRUDOutputAdapter < Droonga::OutputAdapterPlugin
    repository.register("crud", self)

    command :crud_generic_response
    def crud_generic_response(output_message)
      if output_message.status_code == Droonga::Replier::STATUS_OK
        output_message.body = output_message.body["success"]
      end
    end
  end
end
