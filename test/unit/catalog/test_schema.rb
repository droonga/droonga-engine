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
  def create_schema(dataset_name, data)
    Droonga::Catalog::Schema.new(dataset_name, data)
  end

  class SchemaTest < self
    def test_schema_not_specified
      assert_equal([],
                   create_schema("dataset_name", nil).to_messages)
    end

    def test_no_table
      assert_equal([],
                   create_schema("dataset_name", {}).to_messages)
    end

    def test_key_index
      assert_equal([
                     {
                       "type" => "table_create",
                       "dataset" => "dataset_name",
                       "body" => {
                         "name"       => "Term",
                         "key_type"   => "ShortText",
                         "flags"      => "TABLE_PAT_KEY",
                         "normalizer" => "NormalizerAuto",
                       }
                     },
                     {
                       "type" => "table_create",
                       "dataset" => "dataset_name",
                       "body" => {
                         "name"       => "Store",
                         "key_type"   => "ShortText",
                         "flags"      => "TABLE_HASH_KEY",
                       }
                     },
                     {
                       "type" => "column_create",
                       "dataset" => "dataset_name",
                       "body" => {
                         "name"       => "stores__key",
                         "table"      => "Term",
                         "type"       => "Store",
                         "flags"      => "COLUMN_INDEX",
                         "source"     => "_key"
                       }
                     }
                   ],
                   create_schema(
                     "dataset_name",
                     "Term" => {
                       "type"       => "PatriciaTrie",
                       "keyType"    => "ShortText",
                       "normalizer" => "NormalizerAuto",
                       "columns" => {
                         "stores__key" => {
                           "type"      => "Index",
                           "valueType" => "Store",
                           "indexOptions" => {
                             "sources" => [
                               "_key"
                             ]
                           }
                         }
                       }
                     },
                     "Store" => {
                       "keyType" => "ShortText"
                     }
                   ).to_messages)
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

      class TypeTest < self
        def type(data)
          create_table("table_name", data).type
        end

        def test_default
          assert_equal("Hash", type({}))
        end

        def test_array
          create_table("Array", type("Array"))
        end
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

      class TypeTest < self
        def type(data)
          create_column("column_name", data).type
        end

        def test_scalar
          assert_equal("Scalar", type("type" => "Scalar"))
        end

        def test_default
          assert_equal("Scalar", type({}))
        end
      end

      class TypeSymbolTest < self
        def type_symbol(type)
          data = {
            "type" => type,
          }
          create_column("column_name", data).type_symbol
        end

        def test_scalar
          assert_equal(:scalar, type_symbol("Scalar"))
        end

        def test_vector
          assert_equal(:vector, type_symbol("Vector"))
        end

        def test_index
          assert_equal(:index, type_symbol("Index"))
        end
      end

      class ValueType < self
        def value_type(data)
          create_column("column_name", data).value_type
        end

        def test_data_type
          assert_equal("ShortText", value_type("valueType" => "ShortText"))
        end

        def test_reference_type
          assert_equal("Users", value_type("valueType" => "Users"))
        end

        def test_default
          assert_nil(value_type({}))
        end
      end

      class ValueTypeGroonga < self
        def value_type_groonga(type)
          data = {
            "valueType" => type,
          }
          create_column("column_name", data).value_type_groonga
        end

        def test_integer
          assert_equal("Int64", value_type_groonga("Integer"))
        end
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

      class FlagsTest < self
        def flags(data)
          create_column("column_name", data).flags
        end

        def test_type
          data = {
            "type" => "Scalar"
          }
          assert_equal(["COLUMN_SCALAR"],
                       flags(data))
        end

        def test_weight_options
          data = {
            "type" => "Vector",
            "vectorOptions" => {
              "weight" => true
            }
          }
          assert_equal(["COLUMN_VECTOR", "WITH_WEIGHT"],
                       flags(data))
        end

        def test_index_options
          data = {
            "type" => "Index",
            "indexOptions" => {
              "section" => true
            }
          }
          assert_equal(["COLUMN_INDEX", "WITH_SECTION"],
                       flags(data))
        end
      end
    end

    class ColumnVectorOptionsTest < self
      def create_options(data)
        Droonga::Catalog::Schema::ColumnVectorOptions.new(data)
      end

      def test_weight
        data = {
          "weight" => true
        }
        options = create_options(data)
        assert_equal(true, options.weight)
      end

      def test_flags
        data = {
          "weight" => true
        }
        options = create_options(data)
        assert_equal(["WITH_WEIGHT"],
                     options.flags)
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
