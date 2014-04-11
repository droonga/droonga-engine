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

require "droonga/loggable"

module Droonga
  class SchemaApplier
    include Loggable

    def initialize(context, schema)
      @context = context
      @schema = schema
    end

    def apply
      # TODO: Support migration
      Groonga::Schema.define(:context => @context) do |schema|
        create_tables(schema)
        create_reference_columns(schema)
        create_index_columns(schema)
      end
    end

    private
    def each_table
      reference_tables = []
      @schema.tables.each_value do |table|
        if reference_table?(table)
          reference_tables << table
        else
          yield(table)
        end
      end
      reference_tables.each do |table|
        yield(table)
      end
    end

    def reference_table?(table)
      table.type != "Array" and !type_names.include?(table.key_type_groonga)
    end

    def each_column(table)
      table.columns.each_value do |column|
        yield(column)
      end
    end

    def normal_column?(column)
      column.type != "Index" and type_names.include?(column.value_type_groonga)
    end

    def reference_column?(column)
      column.type != "Index" and !type_names.include?(column.value_type_groonga)
    end

    def index_column?(column)
      column.type == "Index"
    end

    def types
      @types ||= collect_available_types
    end

    def type_names
      @type_names ||= types.collect(&:name)
    end

    def collect_available_types
      each_options = {
        :ignore_missing_object => true
      }
      @context.database.each(each_options).find_all do |object|
        object.is_a?(Groonga::Type)
      end
    end

    def create_tables(schema)
      each_table do |table|
        create_table(schema, table)
      end
    end

    def create_table(schema, table)
      options = {
        :type => table.type_symbol,
        :key_type => table.key_type_groonga,
        :default_tokenizer => table.tokenizer,
        :normalizer => table.normalizer,
      }
      schema.create_table(table.name, options) do |table_definition|
        each_column(table) do |column|
          next unless normal_column?(column)
          create_data_column(table_definition, column)
        end
      end
    end

    def create_data_column(table_definition, column)
      options = {
        :type => column.type_symbol,
      }
      if options[:type] == :vector
        options[:with_weight] = true if column.vector_options.weight
      end
      table_definition.column(column.name, column.value_type_groonga, options)
    end

    def create_reference_columns(schema)
      each_table do |table|
        reference_columns = []
        each_column(table) do |column|
          reference_columns << column if reference_column?(column)
        end
        next if reference_columns.empty?

        schema.change_table(table.name) do |table_definition|
          reference_columns.each do |column|
            create_data_column(table_definition, column)
          end
        end
      end
    end

    def create_index_columns(schema)
      each_table do |table|
        index_columns = []
        each_column(table) do |column|
          index_columns << column if index_column?(column)
        end
        next if index_columns.empty?

        schema.change_table(table.name) do |table_definition|
          index_columns.each do |column|
            create_index_column(table_definition, column)
          end
        end
      end
    end

    def create_index_column(table_definition, column)
      sources = column.index_options.sources || []
      options = {
        :with_section  => column.index_options.section,
        :with_weight   => column.index_options.weight,
        :with_position => column.index_options.position,
      }
      table_definition.index(column.value_type_groonga, *sources, options)
    end

    def log_tag
      "schema_applier"
    end
  end
end
