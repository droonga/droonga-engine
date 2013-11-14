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

require "droonga/plugin/handler_add"

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
  def setup_database
    FileUtils.rm_rf(@database_path.dirname.to_s)
    FileUtils.mkdir_p(@database_path.dirname.to_s)
    @database = Groonga::Database.create(:path => @database_path.to_s)
  end

  def setup_schema
  end

  def teardown_database
    @database.close
    @database = nil
    FileUtils.rm_rf(@database_path.dirname.to_s)
  end

  def setup_handler
    @worker = StubWorker.new
    @handler = Droonga::AddHandler.new(@worker)
  end

  def teardown_handler
    @handler = nil
  end

  public
  class HasKeyTest < self
    def setup_schema
      Groonga::Schema.define do |schema|
        schema.create_table("Users",
                            :type => :hash,
                            :key_type => :short_text) do |table|
          table.short_text("country")
        end
      end
    end

    def test_empty_values
      request = {
        "table"  => "Users",
        "key"    => "mori",
        "values" => {},
      }
      mock(@handler).emit([true])
      @handler.add(request)
      table = @worker.context["Users"]
      assert_equal(["mori"], table.collect(&:key))
    end

    def test_values
      request = {
        "table"  => "Users",
        "key"    => "asami",
        "values" => {"country" => "japan"},
      }
      mock(@handler).emit([true])
      @handler.add(request)
      table = @worker.context["Users"]
      assert_equal(["japan"], table.collect(&:country))
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
      mock(@handler).emit([true])
      @handler.add(request)
      table = @worker.context["Books"]
      assert_equal([nil], table.collect(&:title))
    end

    def test_with_values
      request = {
        "table"  => "Books",
        "values" => {"title" => "CSS"},
      }
      mock(@handler).emit([true])
      @handler.add(request)
      table = @worker.context["Books"]
      assert_equal(["CSS"], table.collect(&:title))
    end
  end

  class FailureTest < self
    def test_nonexistent_table
      request = {
        "table"  => "Nonexistent",
        "values" => {},
      }
      mock(@handler).emit([false])
      @handler.add(request)
    end
  end
end
