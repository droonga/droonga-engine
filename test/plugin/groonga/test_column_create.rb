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

class ColumnCreateTest < GroongaHandlerTest
  def test_success
    @handler.table_create({"name" => "Books"})
    @handler.column_create({"table" => "Books", "name" => "title", "type" => "ShortText"})
    assert_equal([true], @worker.body)
  end

  def test_name
    @handler.table_create({"name" => "Books"})
    @handler.column_create({"table" => "Books", "name" => "title", "type" => "ShortText"})
    assert_equal(<<-SCHEMA, dump)
table_create Books TABLE_HASH_KEY --key_type ShortText
column_create Books title COLUMN_SCALAR ShortText
    SCHEMA
  end

  def test_type
    @handler.table_create({"name" => "Books"})
    @handler.column_create({"table" => "Books", "name" => "main_text", "type" => "LongText"})
    assert_equal(<<-SCHEMA, dump)
table_create Books TABLE_HASH_KEY --key_type ShortText
column_create Books main_text COLUMN_SCALAR LongText
    SCHEMA
  end

  class FlagsTest < self
    data({
           "COLUMN_SCALAR" => {
             :flags => "COLUMN_SCALAR",
             :schema => <<-SCHEMA,
column_create Books title COLUMN_SCALAR ShortText
             SCHEMA
           },
           "COLUMN_VECTOR" => {
             :flags => "COLUMN_VECTOR",
             :schema => <<-SCHEMA,
column_create Books title COLUMN_VECTOR ShortText
             SCHEMA
           },
         })
    def test_flags(data)
      request = {
        "table" => "Books",
        "name"  => "title",
        "type"  => "ShortText",
        "flags" => data[:flags],
      }
      @handler.table_create({"name" => "Books"})
      @handler.column_create(request)
      assert_equal("table_create Books TABLE_HASH_KEY --key_type ShortText\n#{data[:schema]}", dump)
    end

    data({
           "WITH_SECTION" => {
             :flags => "COLUMN_INDEX",
             :schema => <<-SCHEMA,
column_create Books entry_title COLUMN_INDEX Books title
             SCHEMA
           },
         })
    def test_index_flags(data)
      request = {
        "table"  => "Books",
        "name"   => "entry_title",
        "type"   => "Books",
        "source" => "title",
        "flags"  => data[:flags],
      }
      @handler.table_create({"name" => "Books"})
      @handler.column_create({"table" => "Books", "name" => "title", "type" => "ShortText"})
      @handler.column_create(request)
      assert_equal("table_create Books TABLE_HASH_KEY --key_type ShortText\ncolumn_create Books title COLUMN_SCALAR ShortText\n\n#{data[:schema]}", dump)
    end
  end
end
