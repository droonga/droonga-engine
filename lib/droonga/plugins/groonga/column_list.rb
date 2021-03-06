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

require "groonga/command/column-list"

require "droonga/plugin"
require "droonga/plugins/groonga/generic_command"

module Droonga
  module Plugins
    module Groonga
      module ColumnList
        HEADER = [
          ["id",     "UInt32"],
          ["name",   "ShortText"],
          ["path",   "ShortText"],
          ["type",   "ShortText"],
          ["flags",  "ShortText"],
          ["domain", "ShortText"],
          ["range",  "ShortText"],
          ["source", "ShortText"],
        ].freeze

        class Command < GenericCommand
          def process_request(request)
            command_class = ::Groonga::Command.find("column_list")
            @command = command_class.new("column_list", request)

            table_name = valid_table_name("table")

            columns = list_columns(table_name)
            [HEADER, *columns]
          end

          private
          def list_columns(table_name)
            table = @context[table_name]
            virtual_columns(table) + real_columns(table)
          end

          def virtual_columns(table)
            if not table.support_key? or table.domain.nil?
              return []
            end
            [
              [
                table.id,          # id
                "_key",            # name
                "",                # path
                "",                # type
                "COLUMN_SCALAR",   # flags
                table.name,        # domain
                table.domain.name, # range
                [],                # source
              ]
            ]
          end

          def real_columns(table)
            table.columns.collect do |column|
              format_column(column)
            end
          end

          def format_column(column)
            [
              column.id,
              column.local_name,
              column.path,
              column_type(column),
              column_flags(column),
              column.domain.name,
              column.range.name,
              column_source(column),
            ]
          end

          def column_type(column)
            case column
            when ::Groonga::FixSizeColumn
              "fix"
            when ::Groonga::VariableSizeColumn
              "var"
            when ::Groonga::IndexColumn
              "index"
            end
          end

          def column_flags(column)
            flags = []
            if column.is_a?(::Groonga::IndexColumn)
              flags << "COLUMN_INDEX"
              flags << "WITH_SECTION" if column.with_section?
              flags << "WITH_WEIGHT" if column.with_weight?
              flags << "WITH_POSITION" if column.with_position?
            else
              if column.scalar?
                flags << "COLUMN_SCALAR"
              elsif column.vector?
                flags << "COLUMN_VECTOR"
              end
              flags << "WITH_WEIGHT" if column.with_weight?
            end
            flags.join('|')
          end

          def column_source(column)
            return [] unless column.is_a?(::Groonga::IndexColumn)
            column.sources.collect do |source|
              if source.is_a?(::Groonga::Table)
                source.name
              else
                source.local_name
              end
            end
          end
        end

        class Handler < Droonga::Handler
          def handle(message)
            command = Command.new(@context)
            command.execute(message.request)
          end
        end

        Groonga.define_single_step do |step|
          step.name = "column_list"
          step.handler = Handler
          step.collector = Collectors::Or
        end
      end
    end
  end
end
