# -*- coding: utf-8 -*-
#
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

require "groonga"
require "groonga/command/table-remove"

module Droonga
  class GroongaHandler
    class TableRemove < Command
      def process_request(request)
        command_class = Groonga::Command.find("table_remove")
        @command = command_class.new("table_remove", request)

        name = @command["name"]
        unless name
          raise CommandError.new(:status => Status::INVALID_ARGUMENT,
                                 :message => "Cannot remove anonymous table",
                                 :result => false)
        end

        Groonga::Schema.define(:context => @context) do |schema|
          schema.remove_table(name)
        end
        true
      end
    end
  end
end
