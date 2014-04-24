# Copyright (C) 2013-2014 Droonga Project
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
  def create_handler
    Droonga::Plugins::Groonga::ColumnCreate::Handler.new("droonga",
                                                         @handler.context,
                                                         @messenger,
                                                         @loop)
  end

  def test_success
    Groonga::Schema.define do |schema|
      schema.create_table("Books", :type => :hash)
    end
    message = {
      "table" => "Books",
      "name"  => "title",
      "type"  => "ShortText",
    }
    response = process(:column_create, message)
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
    response = process(:column_create, message)
    assert_equal(
      [NORMALIZED_HEADER_INVALID_ARGUMENT, false],
      [normalize_header(response.first), response.last]
    )
  end

  def test_name
    Groonga::Schema.define do |schema|
      schema.create_table("Books", :type => :hash)
    end
    process(:column_create,
            {"table" => "Books", "name" => "title", "type" => "ShortText"})
    assert_equal(<<-SCHEMA, dump)
table_create Books TABLE_HASH_KEY --key_type ShortText
column_create Books title COLUMN_SCALAR ShortText
    SCHEMA
  end

  def test_type
    Groonga::Schema.define do |schema|
      schema.create_table("Books", :type => :hash)
    end
    process(:column_create,
            {"table" => "Books", "name" => "main_text", "type" => "LongText"})
    assert_equal(<<-SCHEMA, dump)
table_create Books TABLE_HASH_KEY --key_type ShortText
column_create Books main_text COLUMN_SCALAR LongText
    SCHEMA
  end

  class FlagsTest < self
    class DataStoreTest < self
      data({
             "COLUMN_SCALAR" => {
               :flags => "COLUMN_SCALAR",
             },
             "COLUMN_VECTOR" => {
               :flags => "COLUMN_VECTOR",
             },
             "COLUMN_VECTOR|WITH_WEIGHT" => {
               :flags => "COLUMN_VECTOR|WITH_WEIGHT",
             },
           })
      def test_data_store_column_type(data)
        request = {
          "table" => "Books",
          "name"  => "title",
          "type"  => "ShortText",
          "flags" => data[:flags],
        }
        Groonga::Schema.define do |schema|
          schema.create_table("Books", :type => :hash)
        end
        process(:column_create, request)
        assert_equal(<<-EXPECTED, dump)
table_create Books TABLE_HASH_KEY --key_type ShortText
column_create Books title #{data[:flags]} ShortText
        EXPECTED
      end
    end

    class IndexTest < self
      def setup
        super
        Groonga::Schema.define do |schema|
          schema.create_table("Books", :type => :hash)
        end
        process(:column_create,
                {"table" => "Books", "name" => "title", "type" => "ShortText"})
      end

      def test_index_column_type
        data = {
          :flags  => "COLUMN_INDEX",
        }
        request = {
          "table"  => "Books",
          "name"   => "entry_title",
          "type"   => "Books",
          "source" => "title",
          "flags"  => data[:flags],
        }
        process(:column_create, request)
        assert_equal(<<-EXPECTED, dump)
table_create Books TABLE_HASH_KEY --key_type ShortText
column_create Books title COLUMN_SCALAR ShortText

column_create Books entry_title #{data[:flags]} Books title
        EXPECTED
      end

      data({
             "WITH_SECTION" => {
               :flags => "WITH_SECTION",
             },
             "WITH_WEIGHT" => {
               :flags => "WITH_WEIGHT",
             },
             "WITH_POSITION" => {
               :flags => "WITH_POSITION",
             },
             "COLUMN_INDEX with all" => {
               :flags => "WITH_SECTION|WITH_WEIGHT|WITH_POSITION",
             },
           })
      def test_index_flags(data)
        flags = "COLUMN_INDEX|#{data[:flags]}"
        request = {
          "table"  => "Books",
          "name"   => "entry_title",
          "type"   => "Books",
          "source" => "title",
          "flags"  => flags,
        }
        process(:column_create, request)
        assert_equal(<<-EXPECTED, dump)
table_create Books TABLE_HASH_KEY --key_type ShortText
column_create Books title COLUMN_SCALAR ShortText

column_create Books entry_title #{flags} Books title
        EXPECTED
      end
    end
  end
end
