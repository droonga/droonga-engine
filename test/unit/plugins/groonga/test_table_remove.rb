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
  def create_handler
    Droonga::Plugins::Groonga::TableRemove::Handler.new("droonga",
                                                        @handler.context,
                                                        @messenger,
                                                        @loop)
  end

  def setup
    super
    Groonga::Schema.define do |schema|
      schema.create_table("Books", :type => :hash)
    end
  end

  def test_success
    response = process(:table_remove, {"name" => "Books"})
    assert_equal(
      [NORMALIZED_HEADER_SUCCESS, true],
      [normalize_header(response.first), response.last]
    )
    assert_equal(<<-SCHEMA, dump)
    SCHEMA
  end

  def test_failure
    response = process(:table_remove, {})
    assert_equal(
      [NORMALIZED_HEADER_INVALID_ARGUMENT, false],
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
