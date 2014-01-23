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

class TableRemoveTest < GroongaHandlerTest
  def setup
    super
    process(:table_create, {"name" => "Books"})
  end

  def test_success
    process(:table_remove, {"name" => "Books"})
    response = @messenger.values.last
    assert_equal(
      [[Droonga::GroongaHandler::Status::SUCCESS, NORMALIZED_START_TIME, NORMALIZED_ELAPSED_TIME], true],
      [normalize_header(response.first), response.last]
    )
    assert_equal(<<-SCHEMA, dump)
    SCHEMA
  end

  def test_failure
    process(:table_remove, {})
    response = @messenger.values.last
    assert_equal(
      [[Droonga::GroongaHandler::Status::INVALID_ARGUMENT, NORMALIZED_START_TIME, NORMALIZED_ELAPSED_TIME], false],
      [normalize_header(response.first), response.last]
    )
  end

  def test_remove
    process(:table_remove, {"name" => "Books"})
    assert_equal(<<-SCHEMA, dump)
    SCHEMA
  end

  def test_unknown_table
    process(:table_remove, {"name" => "Unknown"})
    assert_equal(<<-SCHEMA, dump)
table_create Books TABLE_HASH_KEY --key_type ShortText
    SCHEMA
  end
end
