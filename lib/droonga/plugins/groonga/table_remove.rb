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

require "groonga/command/table-remove"

require "droonga/plugin"
require "droonga/plugins/groonga/generic_command"

module Droonga
  module Plugins
    module Groonga
      module TableRemove
        class Command < GenericCommand
          def process_request(request)
            command_class = ::Groonga::Command.find("table_remove")
            @command = command_class.new("table_remove", request)

            name = @command["name"]
            if name.nil? or @context[name].nil?
              raise CommandError.new(:status => Status::INVALID_ARGUMENT,
                                     :message => "table not found",
                                     :result => false)
            end

            ::Groonga::Schema.define(:context => @context) do |schema|
              schema.remove_table(name)
            end
            true
          end
        end

        class Handler < Droonga::Handler
          message.type = "table_remove"
          action.synchronous = true

          def handle(message, messenger)
            command = Command.new(@context)
            outputs = command.execute(message.request)
            messenger.emit(outputs)
          end
        end
      end
    end
  end
end
