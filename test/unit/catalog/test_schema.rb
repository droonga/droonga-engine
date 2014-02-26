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
                   create_schema(nil).to_messages)
    end

    def test_no_table
      assert_equal([],
                   create_schema({}).to_messages)
    end

=begin
    def test_integration
      assert_equal([
                     {
                       "type" => "table_create",
                       "body" => {
                         "name"       => "Keyword",
                         "key_type"   => "ShortText",
                         "flags"      => "TABLE_PAT_KEY",
                         "normalizer" => "NormalizerAuto",
                       }
                     },
                     {
                       "type" => "table_create",
                       "body" => {
                         "name"     => "Query",
                         "key_type" => "ShortText",
                         "flags"    => "TABLE_HASH_KEY"
                       }
                     },
                     {
                       "type" => "table_create",
                       "body" => {
                         "name"     => "Route",
                         "key_type" => "ShortText",
                         "flags"    => "TABLE_HASH_KEY"
                       }
                     },
                     {
                       "type" => "table_create",
                       "body" => {
                         "name"     => "Subscriber",
                         "key_type" => "ShortText",
                         "flags"    => "TABLE_HASH_KEY"
                       }
                     }
                     # TODO add column_create messages
                   ],
                   create_schema(
                     "Keyword" => {
                       "type"       => "PatriciaTrie",
                       "keyType"    => "ShortText",
                       "normalizer" => "NormalizerAuto",
                       "columns" => {
                         "queries" => {
                           "type"      => "Index",
                           "valueType" => "Query",
                           "indexOptions" => {
                             "sources" => [
                               "keywords"
                             ]
                           }
                         },
                       }
                     },
                     "Query" => {
                       "type"    => "Hash",
                       "keyType" => "ShortText",
                       "columns" => {
                         "subscribers" => {
                           "type"      => "Index",
                           "valueType" => "Subscriber",
                           "indexOptions" => {
                             "sources" => [
                               "subscriptions"
                             ]
                           }
                         },
                         "keywords" => {
                           "type"      => "Vector",
                           "valueType" => "Keyword"
                         }
                       }
                     },
                     "Route" => {
                       "type"    => "Hash",
                       "keyType" => "ShortText"
                     },
                     "Subscriber" => {
                       "type"    => "Hash",
                       "keyType" => "ShortText",
                       "columns" => {
                         "last_modified" => {
                           "type"      => "Scalar",
                           "valueType" => "Time"
                         },
                         "subscriptions" => {
                           "type"      => "Vector",
                           "valueType" => "Query"
                         },
                         "route" => {
                           "type"      => "Scalar",
                           "valueType" => "Route"
                         },
                       }
                     }
                   ).to_messages)
    end
=end

    class TableTest < self
      def create_table(name, data)
        Droonga::Catalog::Schema::Table.new(name, data)
      end

      def test_name
        assert_equal("table_name",
                     create_table("table_name",
                                  {}).name)
      end

      def test_type_default
        assert_equal("Hash",
                     create_table("table_name",
                                  {}).type)
      end

      def test_type
        assert_equal("Hash",
                     create_table("table_name",
                                  {
                                    "type" => "Hash"
                                  }).type)
      end

      def test_flags
        assert_equal(["TABLE_HASH_KEY"],
                     create_table("table_name",
                                  {
                                    "type" => "Hash"
                                  }).flags)
      end

      def test_key_type
        assert_equal("ShortText",
                     create_table("table_name",
                                  {
                                    "keyType" => "ShortText"
                                  }).key_type)
      end

      def test_key_type_groonga
        assert_equal("Int64",
                     create_table("table_name",
                                  {
                                    "keyType" => "Integer"
                                  }).key_type_groonga)
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

      def test_to_table_create_body
        assert_equal({
                       "name"              => "table_name",
                       "key_type"          => "ShortText",
                       "flags"             => "TABLE_PAT_KEY",
                       "normalizer"        => "NormalizerAuto",
                       "default_tokenizer" => "TokenBigram"
                     },
                     create_table("table_name",
                                  {
                                    "type"       => "PatriciaTrie",
                                    "keyType"    => "ShortText",
                                    "normalizer" => "NormalizerAuto",
                                    "tokenizer"  => "TokenBigram"
                                  }).to_table_create_body)

      end
    end

    class ColumnTest < self
      def create_column(name, data)
        Droonga::Catalog::Schema::Column.new("table_name", name, data)
      end

      def test_name
        assert_equal("column_name",
                     create_column("column_name",
                                   {}).name)
      end

      def test_type
        assert_equal("Scalar",
                     create_column("column_name",
                                   {
                                     "type" => "Scalar"
                                   }).type)
      end

      def test_type_default
        assert_equal("Scalar",
                     create_column("column_name",
                                   {}).type)
      end

      def test_flags
        assert_equal(["COLUMN_SCALAR"],
                     create_column("column_name",
                                   {
                                     "type" => "Scalar"
                                   }).flags)
      end

      def test_value_type
        assert_equal("ShortText",
                     create_column("column_name",
                                   {
                                     "valueType" => "ShortText"
                                   }).value_type)
      end

      def test_value_type_groonga
        assert_equal("Int64",
                     create_column("column_name",
                                   {
                                     "valueType" => "Integer"
                                   }).value_type_groonga)
      end

      def test_flags_with_column_index_options
        assert_equal(["COLUMN_SCALAR", "WITH_SECTION"],
                     create_column("column_name",
                                   {
                                     "type" => "Scalar",
                                     "indexOptions" => {
                                       "section" => true
                                     }
                                   }).flags)
      end

      def test_to_column_create_body
        assert_equal({
                       "name"  => "column_name",
                       "flags" => "COLUMN_SCALAR",
                       "table" => "table_name",
                       "type"  => "ShortText"
                     },
                     create_column("column_name",
                                  {
                                    "type"      => "Scalar",
                                    "valueType" => "ShortText"
                                  }).to_column_create_body)
      end
    end

    class ColumnIndexOptionsTest < self
      def create_options(data)
        Droonga::Catalog::Schema::ColumnIndexOptions.new(data)
      end

      def test_section
        assert_equal(true,
                     create_options({
                                      "section" => true
                                    }).section)
      end

      def test_weight
        assert_equal(true,
                     create_options({
                                      "weight" => true
                                    }).weight)
      end

      def test_position
        assert_equal(true,
                     create_options({
                                      "position" => true
                                    }).position)
      end

      def test_flags
        assert_equal(["WITH_SECTION"],
                     create_options({
                                      "section" => true
                                    }).flags)
      end
    end
  end
end
