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
end
