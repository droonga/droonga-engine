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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "groonga/command/column-rename"

require "droonga/plugin"
require "droonga/plugins/groonga/generic_command"

module Droonga
  module Plugins
    module Groonga
      module ColumnRename
        class Command < GenericCommand
          def process_request(request)
            command_class = ::Groonga::Command.find("column_rename")
            @command = command_class.new("column_rename", request)

            table_name = valid_table_name("table", :error_result => false)
            column_name = valid_column_name("name", :table_name => table_name,
                                                    :error_result => false)

            new_name = @command["new_name"]

            result = rename_column(table_name, column_name, new_name)
            restart_workers
            result
          end

          private
          def rename_column(table_name, column_name, new_name)
            ::Groonga::Schema.define(:context => @context) do |schema|
              schema.change_table(table_name) do |table|
                table.rename_column(column_name, new_name)
              end
            end
            true
          end
        end

        class Handler < Droonga::Handler
          action.synchronous = true

          def handle(message)
            command = Command.new(@context)
            command.execute(message.request)
          end
        end

        Groonga.define_single_step do |step|
          step.name = "column_rename"
          step.write = true
          step.handler = Handler
          step.collector = Collectors::Or
        end
      end
    end
  end
end
