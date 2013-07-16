# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 droonga project
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
require "groonga/command/column-create"

module Droonga
  class GroongaHandler
    class ColumnCreate
      def initialize(context)
        @context = context
      end

      def execute(request)
        command_class = Groonga::Command.find("column_create")
        @command = command_class.new("column_create", request)

        table_name = @command["table"]
        column_name = @command["name"]
        column_type = @command["type"]

        options = parse_command
        Groonga::Schema.define(:context => @context) do |schema|
          schema.change_table(table_name) do |table|
            table.column(column_name, column_type, options)
          end
        end
        [true]
      end

      private
      def parse_command
        options = {}
        parse_flags(options)
        options
      end

      def parse_flags(options)
        options[:type] = :scalar
        if @command.column_scalar?
          options[:type] = :scalar
        elsif @command.column_vector?
          options[:type] = :vector
        end
        options
      end
    end
  end
end
