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
        def initialize(data)
          @data = data
        end

        def weight
          @data["weight"]
        end
      end

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
      end

      class Column
        attr_reader :table, :name, :data, :vector_options, :index_options
        def initialize(table, name, data)
          @table = table
          @name = name
          @data = data
          @vector_options = ColumnVectorOptions.new(vector_options_data)
          @index_options = ColumnIndexOptions.new(index_options_data)
        end

        def ==(other)
          self.class == other.class and
            name == other.name and
            data == other.data
        end

        def type
          @data["type"] || "Scalar"
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
          @data["valueType"]
        end

        def value_type_groonga
          if value_type == "Integer"
            "Int64"
          else
            value_type
          end
        end

        private
        def vector_options_data
          @data["vectorOptions"] || {}
        end

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
          @data["keyType"]
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
          @data["tokenizer"]
        end

        def normalizer
          @data["normalizer"]
        end

        private
        def columns_data
          @data["columns"] || []
        end
      end

      attr_reader :tables
      def initialize(dataset_name, data)
        @dataset_name = dataset_name
        @data = data || []
        @tables = {}
        @data.each do |table_name, table_data|
          @tables[table_name] = Table.new(table_name, table_data)
        end
      end

      def ==(other)
        self.class == other.class and
          tables == other.tables
      end
    end
  end
end
