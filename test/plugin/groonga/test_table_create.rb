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

class TableCreateTest < GroongaHandlerTest
  def test_success
    @handler.table_create({"name" => "Books"})
    assert_equal([true], @worker.body)
  end

  def test_name
    @handler.table_create({"name" => "Books"})
    assert_equal(<<-SCHEMA, dump)
table_create Books TABLE_HASH_KEY --key_type ShortText
    SCHEMA
  end

  class FlagsTest < self
    def test_table_no_key
      request = {
        "name"  => "Books",
        "flags" => "TABLE_NO_KEY",
      }
      @handler.table_create(request)
      assert_equal(<<-SCHEMA, dump)
table_create Books TABLE_NO_KEY
      SCHEMA
    end

    def test_table_hash_key
      request = {
        "name"  => "Books",
        "flags" => "TABLE_HASH_KEY",
      }
      @handler.table_create(request)
      assert_equal(<<-SCHEMA, dump)
table_create Books TABLE_HASH_KEY --key_type ShortText
      SCHEMA
    end

    def test_table_pat_key
      request = {
        "name"  => "Books",
        "flags" => "TABLE_PAT_KEY",
      }
      @handler.table_create(request)
      assert_equal(<<-SCHEMA, dump)
table_create Books TABLE_PAT_KEY --key_type ShortText
      SCHEMA
    end

    def test_table_dat_key
      request = {
        "name"  => "Books",
        "flags" => "TABLE_DAT_KEY",
      }
      @handler.table_create(request)
      assert_equal(<<-SCHEMA, dump)
table_create Books TABLE_DAT_KEY --key_type ShortText
      SCHEMA
    end

    def test_key_with_sis_with_pat_key
      request = {
        "name"  => "Books",
        "flags" => "KEY_WITH_SIS|TABLE_PAT_KEY",
      }
      @handler.table_create(request)
      assert_equal(<<-SCHEMA, dump)
table_create Books TABLE_PAT_KEY|KEY_WITH_SIS --key_type ShortText
      SCHEMA
    end

    def test_key_with_sis_without_pat_key
      request = {
        "name"  => "Books",
        "flags" => "TABLE_NO_KEY|KEY_WITH_SIS",
      }
      @handler.table_create(request)
      assert_equal(<<-SCHEMA, dump)
table_create Books TABLE_NO_KEY
      SCHEMA
    end
  end

  class KeyTypeTest < self
    def test_key_type
      request = {
        "name"  => "Books",
        "key_type" => "Int32",
      }
      @handler.table_create(request)
      assert_equal(<<-SCHEMA, dump)
table_create Books TABLE_HASH_KEY --key_type Int32
      SCHEMA
    end
  end

  class ValueTypeTest < self
    def test_value_type
      request = {
        "name"  => "Books",
        "value_type" => "Int32",
      }
      @handler.table_create(request)
      assert_equal(<<-SCHEMA, dump)
table_create Books TABLE_HASH_KEY --key_type ShortText --value_type Int32
      SCHEMA
    end
  end

  class DefaultTokenizerTest < self
    def test_default_tokenizer
      request = {
        "name"  => "Books",
        "default_tokenizer" => "TokenBigram",
      }
      @handler.table_create(request)
      assert_equal(<<-SCHEMA, dump)
table_create Books TABLE_HASH_KEY --key_type ShortText --default_tokenizer TokenBigram
      SCHEMA
    end
  end
end
