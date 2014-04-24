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
    Droonga::Plugins::Groonga::ColumnList::Handler.new("droonga",
                                                       @handler.context,
                                                       @messenger,
                                                       @loop)
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
        schema.create_table("Books", :type => :hash)
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
        schema.create_table("Books", :type => :hash)
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
        schema.create_table("Books", :type => :hash)
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
        schema.create_table("Books", :type => :hash)
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
  end
end
