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
    Groonga::Schema.define do |schema|
      schema.create_table("Users",
                          :type => :hash,
                          :key_type => :short_text) do |table|
        table.short_text("country")
      end
    end
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
  def test_add
    request = {
      "table"  => "Users",
      "key"    => "mori",
      "values" => {},
    }
    @handler.add(request)
    table = @worker.context["Users"]
    assert_equal(["mori"], table.collect(&:key))
  end

  def test_add_with_values
    request = {
      "table"  => "Users",
      "key"    => "asami",
      "values" => { "country" => "japan" },
    }
    @handler.add(request)
    table = @worker.context["Users"]
    assert_equal(["japan"], table.collect(&:country))
  end
end
