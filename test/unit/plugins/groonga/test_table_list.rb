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

class TableListTest < GroongaHandlerTest
  TABLES_HEADER = [
    ["id",                "UInt32"],
    ["name",              "ShortText"],
    ["path",              "ShortText"],
    ["flags",             "ShortText"],
    ["domain",            "ShortText"],
    ["range",             "ShortText"],
    ["default_tokenizer", "ShortText"],
    ["normalizer",        "ShortText"],
  ]

  def create_handler
    Droonga::Plugins::Groonga::TableList::Handler.new("droonga",
                                                      @handler.context,
                                                      @messenger,
                                                      @loop)
  end

  def test_success
    Groonga::Schema.define do |schema|
      schema.create_table("Books", :type => :hash)
    end
    response = process(:column_list, {})
    assert_equal(
      NORMALIZED_HEADER_SUCCESS,
      normalize_header(response.first)
    )
  end

  class ListTest < self
    def table_path
      @database_path.to_s + ".0000100"
    end

    def test_hash_table
      Groonga::Schema.define do |schema|
        schema.create_table("Books", :type => :hash)
      end
      response = process(:table_list, {})
      expected = [
        TABLES_HEADER,
        [256,
         "Books",
         table_path,
         "TABLE_HASH_KEY|PERSISTENT",
         "ShortText",
         nil,
         nil,
         nil],
      ]
      assert_equal(expected, response.last)
    end

    def test_array_table
      Groonga::Schema.define do |schema|
        schema.create_table("HistoryEntries", :type => :array)
      end
      response = process(:table_list, {})
      expected = [
        TABLES_HEADER,
        [256,
         "HistoryEntries",
         table_path,
         "TABLE_NO_KEY|PERSISTENT",
         nil,
         nil,
         nil,
         nil],
      ]
      assert_equal(expected, response.last)
    end

    def test_patricia_trie_table
      Groonga::Schema.define do |schema|
        schema.create_table("Books", :type => :patricia_trie)
      end
      response = process(:table_list, {})
      expected = [
        TABLES_HEADER,
        [256,
         "Books",
         table_path,
         "TABLE_PAT_KEY|PERSISTENT",
         "ShortText",
         nil,
         nil,
         nil],
      ]
      assert_equal(expected, response.last)
    end

    def test_double_array_trie_table
      Groonga::Schema.define do |schema|
        schema.create_table("Books", :type => :double_array_trie)
      end
      response = process(:table_list, {})
      expected = [
        TABLES_HEADER,
        [256,
         "Books",
         table_path,
         "TABLE_DAT_KEY|PERSISTENT",
         "ShortText",
         nil,
         nil,
         nil],
      ]
      assert_equal(expected, response.last)
    end

    def test_with_value_type
      Groonga::Schema.define do |schema|
        schema.create_table("BookIds", :type => :hash,
                                       :key_type => "UInt32",
                                       :value_type => "UInt32")
      end
      response = process(:table_list, {})
      expected = [
        TABLES_HEADER,
        [256,
         "BookIds",
         table_path,
         "TABLE_HASH_KEY|PERSISTENT",
         "UInt32",
         "UInt32",
         nil,
         nil],
      ]
      assert_equal(expected, response.last)
    end

    def test_with_default_tokenizer
      Groonga::Schema.define do |schema|
        schema.create_table("Books", :type => :hash,
                                     :default_tokenizer => "TokenBigram")
      end
      response = process(:table_list, {})
      expected = [
        TABLES_HEADER,
        [256,
         "Books",
         table_path,
         "TABLE_HASH_KEY|PERSISTENT",
         "ShortText",
         nil,
         "TokenBigram",
         nil],
      ]
      assert_equal(expected, response.last)
    end

    def test_with_normalizer
      Groonga::Schema.define do |schema|
        schema.create_table("Books", :type => :hash,
                                     :normalizer => "NormalizerAuto")
      end
      response = process(:table_list, {})
      expected = [
        TABLES_HEADER,
        [256,
         "Books",
         table_path,
         "TABLE_HASH_KEY|PERSISTENT",
         "ShortText",
         nil,
         nil,
         "NormalizerAuto"],
      ]
      assert_equal(expected, response.last)
    end
  end
end
