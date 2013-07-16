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
        Groonga::Schema.define(:context => @context) do |schema|
          schema.change_table(table_name) do |table|
            table.column(column_name, column_type, options)
          end
        end
        [true]
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
        target_column = @command["source"]

        options = {}
        Groonga::Schema.define(:context => @context) do |schema|
          schema.change_table(table_name) do |table|
            table.index("#{target_table}.#{target_column}", options)
          end
        end
        [true]
      end
    end
  end
end
