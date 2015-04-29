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

class ColumnListTest < GroongaHandlerTest
  COLUMNS_HEADER = [
    ["id",     "UInt32"],
    ["name",   "ShortText"],
    ["path",   "ShortText"],
    ["type",   "ShortText"],
    ["flags",  "ShortText"],
    ["domain", "ShortText"],
    ["range",  "ShortText"],
    ["source", "ShortText"],
  ]

  def create_handler
    Droonga::Plugins::Groonga::ColumnList::Handler.new(:name      => "droonga",
                                                       :context   => @handler.context,
                                                       :messenger => @messenger,
                                                       :loop      => @loop)
  end

  def virtual_key_column(id, table_name)
    [
      id,
      "_key",
      "",
      "",
      "COLUMN_SCALAR",
      table_name,
      "ShortText",
      [],
    ]
  end

  class HeaderTest < self
    def test_success
      Groonga::Schema.define do |schema|
        schema.create_table("Books", :type => :hash)
        schema.change_table("Books") do |table|
          table.column("title", "ShortText", :type => :scalar)
        end
      end
      message = {
        "table" => "Books",
        "name"  => "title",
      }
      response = process(:column_list, message)
      assert_equal(
        NORMALIZED_HEADER_SUCCESS,
        normalize_header(response.first)
      )
    end

    def test_unknown_table
      message = {
        "table" => "Unknown",
        "name"  => "title",
        "type"  => "ShortText",
      }
      response = process(:column_list, message)
      assert_equal(
        NORMALIZED_HEADER_INVALID_ARGUMENT,
        normalize_header(response.first)
      )
    end
  end

  class BodyTest < self
    def test_fix
      Groonga::Schema.define do |schema|
        schema.create_table("Books", :type => :array)
        schema.change_table("Books") do |table|
          table.column("age", "UInt32", :type => :scalar)
        end
      end
      response = process(:column_list,
                         {"table" => "Books"})
      expected = [
        COLUMNS_HEADER,
        [257,
         "age",
         @database_path.to_s + ".0000101",
         "fix",
         "COLUMN_SCALAR",
         "Books",
         "UInt32",
         []],
      ]
      assert_equal(expected, response.last)
    end

    def test_var
      Groonga::Schema.define do |schema|
        schema.create_table("Books", :type => :array)
        schema.change_table("Books") do |table|
          table.column("title", "ShortText", :type => :scalar)
        end
      end
      response = process(:column_list,
                         {"table" => "Books"})
      expected = [
        COLUMNS_HEADER,
        [257,
         "title",
         @database_path.to_s + ".0000101",
         "var",
         "COLUMN_SCALAR",
         "Books",
         "ShortText",
         []],
      ]
      assert_equal(expected, response.last)
    end

    def test_vector
      Groonga::Schema.define do |schema|
        schema.create_table("Books", :type => :array)
        schema.change_table("Books") do |table|
          table.column("authors", "ShortText", :type => :vector)
        end
      end
      response = process(:column_list,
                         {"table" => "Books"})
      expected = [
        COLUMNS_HEADER,
        [257,
         "authors",
         @database_path.to_s + ".0000101",
         "var",
         "COLUMN_VECTOR",
         "Books",
         "ShortText",
        []],
      ]
      assert_equal(expected, response.last)
    end

    def test_index
      Groonga::Schema.define do |schema|
        schema.create_table("Books", :type => :array)
        schema.change_table("Books") do |table|
          table.column("title", "ShortText", :type => :scalar)
          table.index("Books", "title", :name => "entry_title")
        end
      end
      response = process(:column_list,
                         {"table" => "Books"})
      expected = [
        COLUMNS_HEADER,
        [258,
         "entry_title",
         @database_path.to_s + ".0000102",
         "index",
         "COLUMN_INDEX",
         "Books",
         "Books",
         ["title"]],
        [257,
         "title",
         @database_path.to_s + ".0000101",
         "var",
         "COLUMN_SCALAR",
         "Books",
         "ShortText",
         []],
      ]
      assert_equal(expected, response.last)
    end

    def test_index_source_key
      Groonga::Schema.define do |schema|
        schema.create_table("Memos", :type => :patricia_trie)
        schema.create_table("Terms", :type => :patricia_trie)
        schema.change_table("Terms") do |table|
          table.index("Memos", "_key", :name => "index")
        end
      end
      response = process(:column_list,
                         {"table" => "Terms"})
      expected = [
        COLUMNS_HEADER,
        virtual_key_column(257, "Terms"),
        [258,
         "index",
         @database_path.to_s + ".0000102",
         "index",
         "COLUMN_INDEX",
         "Terms",
         "Memos",
         ["Memos"]],
      ]
      assert_equal(expected, response.last)
    end
  end

  class VirtualColumnsTest < self
    def test_array
      Groonga::Schema.define do |schema|
        schema.create_table("Books", :type => :array)
      end
      response = process(:column_list,
                         {"table" => "Books"})
      expected = [
        COLUMNS_HEADER,
      ]
      assert_equal(expected, response.last)
    end

    def test_hash
      Groonga::Schema.define do |schema|
        schema.create_table("Books", :type => :hash)
      end
      response = process(:column_list,
                         {"table" => "Books"})
      expected = [
        COLUMNS_HEADER,
        virtual_key_column(256, "Books"),
      ]
      assert_equal(expected, response.last)
    end

    def test_patricia_trie
      Groonga::Schema.define do |schema|
        schema.create_table("Books", :type => :patricia_trie)
      end
      response = process(:column_list,
                         {"table" => "Books"})
      expected = [
        COLUMNS_HEADER,
        virtual_key_column(256, "Books"),
      ]
      assert_equal(expected, response.last)
    end

    def test_double_array_trie
      Groonga::Schema.define do |schema|
        schema.create_table("Books", :type => :double_array_trie)
      end
      response = process(:column_list,
                         {"table" => "Books"})
      expected = [
        COLUMNS_HEADER,
        virtual_key_column(256, "Books"),
      ]
      assert_equal(expected, response.last)
    end

    def test_both_virtual_and_real
      Groonga::Schema.define do |schema|
        schema.create_table("Books", :type => :hash)
        schema.change_table("Books") do |table|
          table.column("age", "UInt32", :type => :scalar)
        end
      end
      response = process(:column_list,
                         {"table" => "Books"})
      expected = [
        COLUMNS_HEADER,
        virtual_key_column(256, "Books"),
        [257,
         "age",
         @database_path.to_s + ".0000101",
         "fix",
         "COLUMN_SCALAR",
         "Books",
         "UInt32",
         []],
      ]
      assert_equal(expected, response.last)
    end
  end
end
