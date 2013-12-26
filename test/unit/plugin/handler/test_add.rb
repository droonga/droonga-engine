# Copyright (C) 2013 Droonga Project
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

require "droonga/plugin/handler/add"

class AddHandlerTest < Test::Unit::TestCase
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
    @handler = Droonga::AddHandler.new(@worker)
    @messenger = Droonga::Test::StubHandlerMessenger.new
  end

  def teardown_handler
    @handler = nil
  end

  def process(request)
    message = Droonga::Test::StubHandlerMessage.new(request)
    @handler.add(message, @messenger)
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
      process(request)
      assert_equal([[true]], @messenger.values)
      table = @worker.context["Users"]
      assert_equal(["mori"], table.collect(&:key))
    end

    def test_values
      request = {
        "table"  => "Users",
        "key"    => "asami",
        "values" => {"country" => "japan"},
      }
      process(request)
      assert_equal([[true]], @messenger.values)
      table = @worker.context["Users"]
      assert_equal(["japan"], table.collect(&:country))
    end

    def test_missing_key_parameter
      request = {
        "table"  => "Users",
        "values" => {"country" => "japan"},
      }
      assert_raise(Droonga::AddHandler::MissingPrimaryKeyParameter) do
        process(request)
      end
    end

    def test_invalid_integer_value
      request = {
        "table"  => "Users",
        "values" => {"age" => "secret"},
      }
      assert_raise(Droonga::AddHandler::InvalidValue) do
        process(request)
      end
    end

    def test_invalid_time_value
      request = {
        "table"  => "Users",
        "values" => {"birthday" => "today"},
      }
      assert_raise(Droonga::AddHandler::InvalidValue) do
        process(request)
      end
    end

    def test_unknown_column
      request = {
        "table"  => "Users",
        "values" => {"unknown" => "unknown"},
      }
      assert_raise(Droonga::AddHandler::UnknownColumn) do
        process(request)
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
      process(request)
      assert_equal([[true]], @messenger.values)
      table = @worker.context["Books"]
      assert_equal([nil], table.collect(&:title))
    end

    def test_with_values
      request = {
        "table"  => "Books",
        "values" => {"title" => "CSS"},
      }
      process(request)
      assert_equal([[true]], @messenger.values)
      table = @worker.context["Books"]
      assert_equal(["CSS"], table.collect(&:title))
    end
  end

  class FailureTest < self
    def test_missing_table_parameter
      request = {
        "values" => {},
      }
      assert_raise(Droonga::AddHandler::MissingTableParameter) do
        process(request)
      end
    end

    def test_nonexistent_table
      request = {
        "table"  => "Nonexistent",
        "values" => {},
      }
      assert_raise(Droonga::AddHandler::UnknownTable) do
        process(request)
      end
    end
  end
end
