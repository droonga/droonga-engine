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

require "droonga/catalog/schema"

class CatalogSchemaTest < Test::Unit::TestCase
  private
  def create_schema(data)
    Droonga::Catalog::Schema.new(data)
  end

  class SchemaTest < self
    def test_schema_not_specified
      assert_equal([],
                   create_schema(nil).tables)
    end

    def test_no_table
      assert_equal([],
                   create_schema({}).tables)
    end

    def test_tables
      assert_equal(
        [Droonga::Catalog::Schema::Table.new('table1', {})],
        create_schema(
          "table1" => {
        }).tables
      )
    end

    class TableTest < self
      def create_table(name, data)
        Droonga::Catalog::Schema::Table.new(name, data)
      end

      def test_name
        assert_equal("table_name",
                     create_table("table_name",
                                  {}).name)
      end

      def test_type
        assert_equal("Hash",
                     create_table("table_name",
                                  {
                                    "type" => "Hash"
                                  }).type)
      end

      def test_key_type
        assert_equal("ShortText",
                     create_table("table_name",
                                  {
                                    "keyType" => "ShortText"
                                  }).key_type)
      end

      def test_tokenizer
        assert_equal("TokenBigram",
                     create_table("table_name",
                                  {
                                    "tokenizer" => "TokenBigram"
                                  }).tokenizer)
      end

      def test_normalizer
        assert_equal("NormalizerAuto",
                     create_table("table_name",
                                  {
                                    "normalizer" => "NormalizerAuto"
                                  }).normalizer)
      end
    end
  end
end
