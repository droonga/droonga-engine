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
        attr_reader :name, :data, :index_options
        def initialize(name, data)
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
          when "Scalar"
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

          @columns = columns_data.map do |column_name, column_data|
            Column.new(column_name, column_data)
          end
        end

        def ==(other)
          self.class == other.class and
            name == other.name and
            data == other.data
        end

        def type
          @data["type"]
        end

        def key_type
          @data["keyType"]
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
            "key_type" => key_type,
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

      attr_reader :tables
      def initialize(data)
        @data = data || []
        @tables = @data.map do |table_name, table_data|
          Table.new(table_name, table_data)
        end
      end

      def to_commands
        commands = tables.map do |table|
          {
            "type" => "table_create",
            "body" => table.to_table_create_body
          }
        end

        # TODO append topologically sorted column_create commands

        commands
      end

      def ==(other)
        self.class == other.class and
          tables == other.tables
      end
    end
  end
end
