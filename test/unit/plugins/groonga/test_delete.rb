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

class DeleteTest < GroongaHandlerTest
  def create_handler
    Droonga::Plugins::Groonga::Delete::Handler.new("droonga",
                                                   @handler.context,
                                                   @messages,
                                                   @loop)
  end

  def test_success
    Groonga::Schema.define do |schema|
      schema.create_table("Books", :type => :hash)
    end
    Groonga::Context.default["Books"].add("sample")
    message = {
      "table" => "Books",
      "key"   => "sample",
    }
    response = process(:delete, message)
    assert_equal(
      [NORMALIZED_HEADER_SUCCESS, true],
      [normalize_header(response.first), response.last]
    )
  end

  def test_unknown_table
    message = {
      "table" => "Unknown",
    }
    response = process(:delete, message)
    assert_equal(
      [NORMALIZED_HEADER_INVALID_ARGUMENT, false],
      [normalize_header(response.first), response.last]
    )
  end

  def test_no_identifier
    Groonga::Schema.define do |schema|
      schema.create_table("Books", :type => :hash)
    end
    message = {
      "table" => "Books",
    }
    response = process(:delete, message)
    assert_equal(
      [NORMALIZED_HEADER_INVALID_ARGUMENT, false],
      [normalize_header(response.first), response.last]
    )
  end

  data(:key_and_id => { "key" => "key", "id" => "1" },
       :id_and_filter => { "id" => "1", "filter" => "filter" },
       :key_and_filter => { "key" => "key", "filter" => "filter" })
  def test_duplicated_identifier(data)
    Groonga::Schema.define do |schema|
      schema.create_table("Books", :type => :hash)
    end
    message = {
      "table" => "Books",
    }.merge(data)
    response = process(:delete, message)
    assert_equal(
      [NORMALIZED_HEADER_INVALID_ARGUMENT, false],
      [normalize_header(response.first), response.last]
    )
  end

  class DeleteTest < self
    def test_key
      Groonga::Schema.define do |schema|
        schema.create_table("Books", :type => :hash)
      end
      Groonga::Context.default["Books"].add("sample")
      process(:delete,
              {"table" => "Books", "key" => "sample"})
      assert_equal(<<-DUMP, dump)
table_create Books TABLE_HASH_KEY --key_type ShortText
      DUMP
    end

    def test_id
      Groonga::Schema.define do |schema|
        schema.create_table("Ages", :type => :array)
      end
      Groonga::Context.default["Ages"].add([])
      process(:delete,
              {"table" => "Ages", "id" => 1})
      assert_equal(<<-DUMP, dump)
table_create Ages TABLE_NO_KEY
      DUMP
    end

    def test_filter
      Groonga::Schema.define do |schema|
        schema.create_table("Books", :type => :hash)
      end
      table = Groonga::Context.default["Books"]
      table.add("Groonga")
      table.add("Droonga")
      process(:delete,
              {"table" => "Books", "filter" => '_key @^ "D"'})
      assert_equal(<<-DUMP, dump)
table_create Books TABLE_HASH_KEY --key_type ShortText

load --table Books
[
["_key"],
["Groonga"]
]
      DUMP
    end
  end
end
