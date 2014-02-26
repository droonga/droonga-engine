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

require "tsort"

module Droonga
  module Catalog
    class Schema
      class ColumnIndexOptions
        def initialize(data)
          @data = data
        end

        def section
          @data["section"]
        end

        def weight
          @data["weight"]
        end

        def position
          @data["position"]
        end

        def sources
          @data["sources"]
        end

        def flags
          flags = []
          flags << "WITH_SECTION"  if section
          flags << "WITH_WEIGHT"   if weight
          flags << "WITH_POSITION" if position
          flags
        end
      end

      class Column
        attr_reader :table, :name, :data, :index_options
        def initialize(table, name, data)
          @table = table
          @name = name
          @data = data
          @index_options = ColumnIndexOptions.new(index_options_data)
        end

        def ==(other)
          self.class == other.class and
            name == other.name and
            data == other.data
        end

        def type
          @data["type"]
        end

        def type_flag
          case type
          when "Scalar"
            "COLUMN_SCALAR"
          when "Vector"
            "COLUMN_VECTOR"
          when "Index"
            "COLUMN_INDEX"
          else
            # TODO raise appropriate error
          end
        end

        def flags
          [type_flag] + index_options.flags
        end

        def value_type
          @data["valueType"]
        end

        def to_column_create_body
          body = {
            "name"  => name,
            "table" => table,
            "flags" => flags.join("|"),
          }
          sources = index_options.sources
          if sources
            body["source"] = sources.join(",")
          end

          body
        end

        private
        def index_options_data
          @data["indexOptions"] || {}
        end
      end

      class Table
        attr_reader :name, :columns, :data
        def initialize(name, data)
          @name = name
          @data = data
          @columns = {}

          columns_data.each do |column_name, column_data|
            @columns[column_name] = Column.new(name, column_name, column_data)
          end
        end

        def ==(other)
          self.class == other.class and
            name == other.name and
            data == other.data
        end

        def type
          @data["type"] || "Hash"
        end

        def key_type
          @data["keyType"]
        end

        def key_type_groonga
          case key_type
          when "Integer"
            "Int64"
          when "Float", "Time", "ShortText", "TokyoGeoPoint", "WGS84GeoPoint"
            key_type
          else
            # TODO raise appropriate error
          end
        end

        def tokenizer
          @data["tokenizer"]
        end

        def normalizer
          @data["normalizer"]
        end

        def type_flag
          case type
          when "Array"
            "TABLE_NO_KEY"
          when "Hash"
            "TABLE_HASH_KEY"
          when "PatriciaTrie"
            "TABLE_PAT_KEY"
          when "DoubleArrayTrie"
            "TABLE_DAT_KEY"
          else
            # TODO raise appropriate error
          end
        end

        def flags
          [type_flag]
        end

        def to_table_create_body
          body = {
            "name"     => name,
            "key_type" => key_type_groonga,
            "flags"    => flags.join("|")
          }

          if tokenizer
            body["default_tokenizer"] = tokenizer
          end

          if normalizer
            body["normalizer"] = normalizer
          end

          body
        end

        private
        def columns_data
          @data["columns"] || []
        end
      end

      class ColumnCreateSorter
        include TSort

        def initialize(tables)
          @tables = tables
        end

        def all_columns
          @tables.values.collect {|table| table.columns.values}.flatten
        end

        def tsort_each_node(&block)
          all_columns.each(&block)
        end

        def tsort_each_child(column, &block)
          dependent_column_names = column.index_options.sources || []
          reference_table = @tables[column.value_type]
          dependent_columns = dependent_column_names.collect do |column_name|
            reference_table.columns[column_name]
          end
          dependent_columns.each(&block)
        end
      end

      attr_reader :tables
      def initialize(data)
        @data = data || []
        @tables = {}
        @data.each do |table_name, table_data|
          @tables[table_name] = Table.new(table_name, table_data)
        end
      end

      def to_messages
        messages = []

        tables.each do |name, table|
          messages << {
            "type" => "table_create",
            "body" => table.to_table_create_body
          }
        end

        sorter = ColumnCreateSorter.new(tables)
        columns = sorter.tsort
        # TODO handle TSort::Cyclic

        columns.each do |column|
          messages << {
            "type" => "column_create",
            "body" => column.to_column_create_body
          }
        end

        messages
      end

      def ==(other)
        self.class == other.class and
          tables == other.tables
      end
    end
  end
end
