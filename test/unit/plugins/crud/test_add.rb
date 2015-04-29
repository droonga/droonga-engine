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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "droonga/plugins/crud"

class CRUDAddHandlerTest < Test::Unit::TestCase
  SUCCESS_RESPONSE_BODY = true

  def setup
    setup_database
    setup_schema
    setup_handler
  end

  def teardown
    teardown_handler
    teardown_database
  end

  private
  def setup_schema
  end

  def setup_handler
    @worker = StubWorker.new
    @messenger = Droonga::Test::StubHandlerMessenger.new
    @loop = nil
    @handler = Droonga::Plugins::CRUD::Handler.new(:name      => "name",
                                                   :context   => @worker.context,
                                                   :messenger => @messenger,
                                                   :loop      => @loop)
  end

  def teardown_handler
    @handler = nil
  end

  def process(request)
    message = Droonga::Test::StubHandlerMessage.new(request)
    @handler.handle(message)
  end

  public
  class HasKeyTest < self
    def setup_schema
      Groonga::Schema.define do |schema|
        schema.create_table("Users",
                            :type => :hash,
                            :key_type => :short_text) do |table|
          table.short_text("country")
          table.int32("age")
          table.time("birthday")
        end
      end
    end

    def test_empty_values
      request = {
        "table"  => "Users",
        "key"    => "mori",
        "values" => {},
      }
      response = process(request)
      assert_equal(SUCCESS_RESPONSE_BODY, response)
      table = @worker.context["Users"]
      assert_equal(["mori"], table.collect(&:key))
    end

    def test_values
      request = {
        "table"  => "Users",
        "key"    => "mori",
        "values" => {"country" => "japan"},
      }
      response = process(request)
      assert_equal(SUCCESS_RESPONSE_BODY, response)
      table = @worker.context["Users"]
      assert_equal(["japan"], table.collect(&:country))
    end

    def test_missing_key_parameter
      request = {
        "table"  => "Users",
        "values" => {"country" => "japan"},
      }
      assert_raise(Droonga::Plugins::CRUD::Handler::MissingPrimaryKeyParameter) do
        process(request)
      end
    end

    def test_invalid_integer_value
      request = {
        "table"  => "Users",
        "key"    => "mori",
        "values" => {"age" => "secret"},
      }
      assert_raise(Droonga::Plugins::CRUD::Handler::InvalidValue) do
        process(request)
      end
    end

    def test_invalid_time_value
      request = {
        "table"  => "Users",
        "key"    => "mori",
        "values" => {"birthday" => "today"},
      }
      assert_raise(Droonga::Plugins::CRUD::Handler::InvalidValue) do
        process(request)
      end
    end

    def test_unknown_column
      request = {
        "table"  => "Users",
        "key"    => "mori",
        "values" => {"unknown" => "unknown"},
      }
      assert_raise(Droonga::Plugins::CRUD::Handler::UnknownColumn) do
        process(request)
      end
    end
  end

  class MismatchedTypeKeyTest < self
    class Acceptable < self
      def test_integer_for_string
        setup_table_with_key_type("ShortText")
        request = {
          "table"  => "Users",
          "key"    => 1,
          "values" => {},
        }
        response = process(request)
        assert_equal(SUCCESS_RESPONSE_BODY, response)
        table = @worker.context["Users"]
        assert_equal(["1"], table.collect(&:key))
      end

      def test_string_for_integer
        setup_table_with_key_type("UInt32")
        request = {
          "table"  => "Users",
          "key"    => "1",
          "values" => {},
        }
        response = process(request)
        assert_equal(SUCCESS_RESPONSE_BODY, response)
        table = @worker.context["Users"]
        assert_equal([1], table.collect(&:key))
      end
    end

    private
    def setup_table_with_key_type(key_type)
      Groonga::Schema.define do |schema|
        schema.create_table("Users",
                            :type => :hash,
                            :key_type => key_type) do |table|
        end
      end
    end
  end

  class NoKeyTest < self
    def setup_schema
      Groonga::Schema.define do |schema|
        schema.create_table("Books",
                            :type => :array) do |table|
          table.short_text("title")
        end
      end
    end

    def test_empty_values
      request = {
        "table"  => "Books",
        "values" => {},
      }
      response = process(request)
      assert_equal(SUCCESS_RESPONSE_BODY, response)
      table = @worker.context["Books"]
      assert_equal([nil], table.collect(&:title))
    end

    def test_with_values
      request = {
        "table"  => "Books",
        "values" => {"title" => "CSS"},
      }
      response = process(request)
      assert_equal(SUCCESS_RESPONSE_BODY, response)
      table = @worker.context["Books"]
      assert_equal(["CSS"], table.collect(&:title))
    end
  end

  class FailureTest < self
    def test_missing_table_parameter
      request = {
        "values" => {},
      }
      assert_raise(Droonga::Plugins::CRUD::Handler::MissingTableParameter) do
        process(request)
      end
    end

    def test_nonexistent_table
      request = {
        "table"  => "Nonexistent",
        "values" => {},
      }
      assert_raise(Droonga::Plugins::CRUD::Handler::UnknownTable) do
        process(request)
      end
    end
  end
end
