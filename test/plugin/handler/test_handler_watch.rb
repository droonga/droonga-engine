# -*- coding: utf-8 -*-
#
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
    top_directory_path = File.join(File.dirname(__FILE__), "..", "..", "..")
    ddl_path = File.join(top_directory_path, "ddl", "watchdb.grn")
    File.open(ddl_path) do |ddl|
      Groonga::Context.default.restore(ddl)
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
  class SubscribeTest < self
    def test_subscribe
      request = {
        "route" => "localhost:23003/output",
        "condition" => "たいやき",
        "subscriber" => "localhost"
      }
      mock(@handler).emit([true])
      @handler.subscribe(request)

      assert_equal(
        ["localhost:23003/output"],
        actual_routes_for_query("たいやき")
      )
    end

    def test_subscribe_route_omitted_from_specified
      request = {
        "condition" => "たいやき",
        "subscriber" => "localhost"
      }
      @worker.envelope["from"] = "localhost:23004/output"
      mock(@handler).emit([true])
      @handler.subscribe(request)

      assert_equal(
        ["localhost:23004/output"],
        actual_routes_for_query("たいやき")
      )
    end

    def test_subscribe_both_route_and_from_specified
      request = {
        "condition" => "たいやき",
        "subscriber" => "localhost",
        "route" => "localhost:23003/output"
      }
      @worker.envelope["from"] = "localhost:23004/output"
      mock(@handler).emit([true])
      @handler.subscribe(request)

      assert_equal(
        ["localhost:23003/output"],
        actual_routes_for_query("たいやき")
      )
    end

    private
    def actual_routes_for_query(query)
      @worker.context["Subscriber"].select {|record|
        record[:subscriptions] =~ query.to_json
      }.map {|subscriber|
        subscriber.route.key
      }
    end
  end

  class UnsubscribeTest < self
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

  class FeedTest < self
    def setup
      super
      setup_subscription
    end

    def test_feed_match
      request = {
        "targets" => {
          "text" => "たいやきおいしいです"
        }
      }
      @handler.feed(request)
      assert_equal(request, @worker.body)
      assert_equal({"to" => ["localhost"]}, @worker.envelope)
    end

    def test_feed_not_match
      request = {
        "targets" => {
          "text" => "たこやきおいしいです"
        }
      }
      @handler.feed(request)
      assert_nil(@worker.body)
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
