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

require "groonga/command/column-create"

require "droonga/plugin"
require "droonga/plugins/groonga/generic_command"

module Droonga
  module Plugins
    module Groonga
      module ColumnCreate
        class Command < GenericCommand
          def process_request(request)
            command_class = ::Groonga::Command.find("column_create")
            @command = command_class.new("column_create", request)

            table_name = @command["table"]
            if table_name.nil? or @context[table_name].nil?
              message = "table doesn't exist: <#{table_name.to_s}>"
              raise CommandError.new(:status => Status::INVALID_ARGUMENT,
                                     :message => message,
                                     :result => false)
            end

            if @command.column_index?
              define_index
            else
              define_column
            end
          end

          private
          def define_column
            table_name = @command["table"]
            column_name = @command["name"]
            column_type = @command["type"]

            options = create_column_options
            ::Groonga::Schema.define(:context => @context) do |schema|
              schema.change_table(table_name) do |table|
                table.column(column_name, column_type, options)
              end
            end
            true
          end

          def create_column_options
            options = {}
            create_column_options_flags(options)
            options
          end

          def create_column_options_flags(options)
            options[:type] = :scalar
            if @command.column_scalar?
              options[:type] = :scalar
            elsif @command.column_vector?
              options[:type] = :vector
            end
            options
          end

          def define_index
            table_name = @command["table"]
            target_table = @command["type"]
            target_columns = (@command["source"] || "").split(/\s*,\s*/)

            options = create_index_options
            ::Groonga::Schema.define(:context => @context) do |schema|
              schema.change_table(table_name) do |table|
                arguments = [target_table, *target_columns]
                arguments << options
                table.index(*arguments)
              end
            end
            true
          end

          def create_index_options
            options = {}
            create_index_options_name(options)
            create_index_options_flags(options)
            options
          end

          def create_index_options_name(options)
            options[:name] = @command["name"]
          end

          def create_index_options_flags(options)
            options[:with_section] = true if @command.with_section?
            options[:with_weight] = true if @command.with_weight?
            options[:with_position] = true if @command.with_position?
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
          step.name = "column_create"
          step.write = true
          step.handler = Handler
          step.collector = Collectors::Add
        end
      end
    end
  end
end
