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

class ColumnRemoveTest < GroongaHandlerTest
  def create_handler
    Droonga::Plugins::Groonga::ColumnRemove::Handler.new("droonga",
                                                         @handler.context,
                                                         @messenger,
                                                         @loop)
  end

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
    response = process(:column_remove, message)
    assert_equal(
      [NORMALIZED_HEADER_SUCCESS, true],
      [normalize_header(response.first), response.last]
    )
  end

  def test_unknown_table
    message = {
      "table" => "Unknown",
      "name"  => "title",
      "type"  => "ShortText",
    }
    response = process(:column_remove, message)
    assert_equal(
      [NORMALIZED_HEADER_INVALID_ARGUMENT, false],
      [normalize_header(response.first), response.last]
    )
  end

  def test_unknown_column
    Groonga::Schema.define do |schema|
      schema.create_table("Books", :type => :hash)
    end
    message = {
      "table" => "Books",
      "name"  => "title",
    }
    response = process(:column_remove, message)
    assert_equal(
      [NORMALIZED_HEADER_INVALID_ARGUMENT, false],
      [normalize_header(response.first), response.last]
    )
  end

  def test_remove
    Groonga::Schema.define do |schema|
      schema.create_table("Books", :type => :hash)
      schema.change_table("Books") do |table|
        table.column("title", "ShortText", :type => :scalar)
      end
    end
    process(:column_remove,
            {"table" => "Books", "name" => "title"})
    assert_equal(<<-SCHEMA, dump)
table_create Books TABLE_HASH_KEY --key_type ShortText
    SCHEMA
  end

  def test_remove_index
    Groonga::Schema.define do |schema|
      schema.create_table("Books", :type => :hash)
      schema.change_table("Books") do |table|
        table.column("title", "ShortText", :type => :scalar)
        table.index("Books", "title", :name => "entry_title")
      end
    end
    process(:column_remove,
            {"table" => "Books", "name" => "title"})
    assert_equal(<<-SCHEMA, dump)
table_create Books TABLE_HASH_KEY --key_type ShortText
    SCHEMA
  end
end
