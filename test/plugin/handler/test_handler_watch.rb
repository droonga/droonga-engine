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

require "droonga/plugin/handler_watch"

class WatchHandlerTest < Test::Unit::TestCase
  def setup
    setup_database
    setup_handler
    setup_schema
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
      schema.create_table("Route",
                          :type => :hash) do |table|
      end

      schema.create_table("Keyword",
                          :type => :patricia_trie,
                          :normalizer => "NormalizerAuto") do |table|
      end

      schema.create_table("Query",
                          :type => :hash) do |table|
        table.column("keywords", "Keyword", :type => :vector)
      end

      schema.create_table("Subscriber",
                          :type => :hash) do |table|
        table.column("subscriptions", "Query", :type => :vector)
        table.column("route", "Route")
      end

      schema.change_table("Query") do |table|
        table.index("Subscriber.subscriptions", :name => "subscribers")
      end

      schema.change_table("Keyword") do |table|
        table.index("Query.keywords", :name => "queries")
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
    @handler = Droonga::WatchHandler.new(@worker)
  end

  def teardown_handler
    @handler = nil
  end

  public
  class TestSubscribe < self
    def test_subscribe
      request = {
        "route" => "localhost:23003/output",
        "condition" => "たいやき",
        "subscriber" => "localhost"
      }
      mock(@handler).emit([true])
      @handler.subscribe(request)
    end
  end

  class TestUnsubscribe < self
    def setup
      super
      setup_subscription
    end

    def test_unsubscribe
      request = {
        "route" => "localhost:23003/output",
        "condition" => "たいやき",
        "subscriber" => "localhost"
      }
      mock(@handler).emit([true])
      @handler.unsubscribe(request)
    end

    private
    def setup_subscription
      request = {
        "route" => "localhost:23003/output",
        "condition" => "たいやき",
        "subscriber" => "localhost"
      }
      stub(@handler).emit([true])
      @handler.subscribe(request)
    end
  end
end
