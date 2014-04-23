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
    ["id", "UInt32"],
    ["name","ShortText"],
    ["path","ShortText"],
    ["type","ShortText"],
    ["flags","ShortText"],
    ["domain", "ShortText"],
    ["range", "ShortText"],
    ["source","ShortText"],
  ]

  def create_handler
    Droonga::Plugins::Groonga::ColumnList::Handler.new("droonga",
                                                       @handler.context,
                                                       @messages,
                                                       @loop)
  end

  def test_success
    Groonga::Schema.define(:context => @context) do |schema|
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
      [NORMALIZED_HEADER_INVALID_ARGUMENT, "table doesnt' exist: <Unknown>"],
      [normalize_header(response.first), response.last]
    )
  end

  class ListTest < self
    def test_fix_column
      Groonga::Schema.define(:context => @context) do |schema|
        schema.create_table("Books", :type => :hash)
        schema.change_table("Books") do |table|
          table.column("title", "ShortText", :type => :scalar)
        end
      end
      response = process(:column_list,
                         {"table" => "Books"})
      expected = [
        COLUMNS_HEADER,
        [257, "title", "fix", "ShortText", "COLUMN_SCALAR", "Foo", "ShortText", []],
      ]
      assert_equal(expected, response.last)
    end

    def test_var_column
      Groonga::Schema.define(:context => @context) do |schema|
        schema.create_table("Books", :type => :hash)
        schema.change_table("Books") do |table|
          table.column("authors", "ShortText", :type => :vector)
        end
      end
      response = process(:column_list,
                         {"table" => "Books"})
      expected = [
        COLUMNS_HEADER,
        [257, "authors", "var", "ShortText", "COLUMN_SCALAR", "Foo", "ShortText", []],
      ]
      assert_equal(expected, response.last)
    end

    def test_index_column
      Groonga::Schema.define(:context => @context) do |schema|
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
        [257, "title", "fix", "ShortText", "COLUMN_SCALAR", "Foo", "ShortText", []],
        [258, "entry_title", "index", "ShortText", "COLUMN_INDEX", "Foo", "Foo", ["Foo.age"]],
      ]
      assert_equal(expected, response.last)
    end
  end
end
