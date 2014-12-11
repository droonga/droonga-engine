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

require "tsort"

module Droonga
  module Catalog
    class Schema
      class ColumnVectorOptions
        def initialize(raw)
          @raw = raw
        end

        def weight
          @raw["weight"]
        end
      end

      class ColumnIndexOptions
        def initialize(raw)
          @raw = raw
        end

        def section
          @raw["section"]
        end

        def weight
          @raw["weight"]
        end

        def position
          @raw["position"]
        end

        def sources
          @raw["sources"]
        end
      end

      class Column
        attr_reader :table, :name, :raw, :vector_options, :index_options
        def initialize(table, name, raw)
          @table = table
          @name = name
          @raw = raw
          @vector_options = ColumnVectorOptions.new(raw_vector_options)
          @index_options = ColumnIndexOptions.new(raw_index_options)
        end

        def ==(other)
          self.class == other.class and
            name == other.name and
            raw == other.raw
        end

        def type
          @raw["type"] || "Scalar"
        end

        def type_symbol
          case type
          when "Scalar"
            :scalar
          when "Vector"
            :vector
          when "Index"
            :index
          end
        end

        def value_type
          @raw["valueType"]
        end

        def value_type_groonga
          if value_type == "Integer"
            "Int64"
          else
            value_type
          end
        end

        private
        def raw_vector_options
          @raw["vectorOptions"] || {}
        end

        def raw_index_options
          @raw["indexOptions"] || {}
        end
      end

      class Table
        attr_reader :name, :columns, :raw
        def initialize(name, raw)
          @name = name
          @raw = raw
          @columns = {}

          raw_columns.each do |column_name, raw_column|
            @columns[column_name] = Column.new(name, column_name, raw_column)
          end
        end

        def ==(other)
          self.class == other.class and
            name == other.name and
            raw == other.raw
        end

        def type
          @raw["type"] || "Hash"
        end

        def type_symbol
          case type
          when "Array"
            :array
          when "Hash"
            :hash
          when "PatriciaTrie"
            :patricia_trie
          when "DoubleArrayTrie"
            :double_array_trie
          end
        end

        def key_type
          @raw["keyType"]
        end

        def key_type_groonga
          case key_type
          when "Integer"
            "Int64"
          when "Float", "Time", "ShortText", "TokyoGeoPoint", "WGS84GeoPoint"
            key_type
          else
            key_type
          end
        end

        def tokenizer
          @raw["tokenizer"]
        end

        def normalizer
          @raw["normalizer"]
        end

        private
        def raw_columns
          @raw["columns"] || []
        end
      end

      attr_reader :tables
      def initialize(dataset_name, raw)
        @dataset_name = dataset_name
        @raw = raw || []
        @tables = {}
        @raw.each do |table_name, raw_table|
          @tables[table_name] = Table.new(table_name, raw_table)
        end
      end

      def ==(other)
        self.class == other.class and
          tables == other.tables
      end
    end
  end
end
