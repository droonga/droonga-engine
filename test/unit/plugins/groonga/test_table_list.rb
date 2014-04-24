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

class TableListTest < GroongaHandlerTest
  TABLES_HEADER = [
    ["id", "UInt32"],
    ["name","ShortText"],
    ["path","ShortText"],
    ["flags","ShortText"],
    ["domain", "ShortText"],
    ["range", "ShortText"],
    ["default_tokenizer","ShortText"],
    ["normalier","ShortText"],
  ]

  def create_handler
    Droonga::Plugins::Groonga::TableList::Handler.new("droonga",
                                                      @handler.context,
                                                      @messages,
                                                      @loop)
  end

  def test_success
    Groonga::Schema.define(:context => @context) do |schema|
      schema.create_table("Books", :type => :hash)
    end
    response = process(:column_list, {})
    assert_equal(
      NORMALIZED_HEADER_SUCCESS,
      normalize_header(response.first)
    )
  end

  class ListTest < self
    def test_hash_table
      Groonga::Schema.define(:context => @context) do |schema|
        schema.create_table("Books", :type => :hash)
      end
      response = process(:table_list, {})
      expected = [
        COLUMNS_HEADER,
        [257,
         "Books",
         @database_path.to_s + ".0000101",
         "TABLE_HASH_KEY|PERSISTENT",
         "ShortText",
         nil,
         nil,
         nil],
      ]
      assert_equal(expected, response.last)
    end

    def test_array_table
      Groonga::Schema.define(:context => @context) do |schema|
        schema.create_table("HistoryEntries", :type => :array)
      end
      response = process(:table_list, {})
      expected = [
        COLUMNS_HEADER,
        [257,
         "HistoryEntries",
         @database_path.to_s + ".0000101",
         "TABLE_NO_KEY|PERSISTENT",
         nil,
         nil,
         nil,
         nil],
      ]
      assert_equal(expected, response.last)
    end
  end
end
