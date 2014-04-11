# -*- coding: utf-8 -*-
#
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
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

require "droonga/plugins/watch"

class WatchHandlerTest < Test::Unit::TestCase
  include WatchHelper

  SUCCESS_RESULT = true

  def setup
    setup_database
    setup_schema
    setup_plugin
  end

  def teardown
    teardown_plugin
    teardown_database
  end

  private
  def setup_plugin
    @handler = Droonga::Test::StubHandler.new
    @messenger = Droonga::Test::StubHandlerMessenger.new
    @loop = nil
  end

  def teardown_plugin
    @plugin = nil
  end

  def process(command, request, headers={})
    message = Droonga::Test::StubHandlerMessage.new(request, headers)
    create_plugin.handle(message)
  end

  public
  class SubscribeTest < self
    def create_plugin
      Droonga::Plugins::Watch::SubscribeHandler.new("droonga",
                                                    @handler.context,
                                                    @messenger,
                                                    @loop)
    end

    def test_subscribe
      request = {
        "route" => "localhost:23003/output",
        "condition" => "たいやき",
        "subscriber" => "localhost"
      }
      response = process(:subscribe, request)
      assert_equal(SUCCESS_RESULT, response)

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
      response = process(:subscribe, request, "from" => "localhost:23004/output")
      assert_equal(SUCCESS_RESULT, response)

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
      response = process(:subscribe, request, "from" => "localhost:23004/output")
      assert_equal(SUCCESS_RESULT, response)

      assert_equal(
        ["localhost:23003/output"],
        actual_routes_for_query("たいやき")
      )
    end

    private
    def actual_routes_for_query(query)
      @handler.context["Subscriber"].select {|record|
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

    def create_plugin
      Droonga::Plugins::Watch::UnsubscribeHandler.new("droonga",
                                                      @handler.context,
                                                      @messenger,
                                                      @loop)
    end

    def test_unsubscribe
      request = {
        "route" => "localhost:23003/output",
        "condition" => "たいやき",
        "subscriber" => "localhost"
      }
      response = process(:unsubscribe, request)
      assert_equal(SUCCESS_RESULT, response)
    end

    private
    def setup_subscription
      request = {
        "route" => "localhost:23003/output",
        "condition" => "たいやき",
        "subscriber" => "localhost"
      }
      response = process(:subscribe, request)
      assert_equal(SUCCESS_RESULT, response)
    end
  end

  class FeedTest < self
    def setup
      super
      setup_subscription
    end

    def create_plugin
      Droonga::Plugins::Watch::FeedHandler.new("droonga",
                                               @handler.context,
                                               @messenger,
                                               @loop)
    end

    def test_feed_match
      request = {
        "targets" => {
          "text" => "たいやきおいしいです"
        }
      }
      process(:feed, request)
      assert_equal([
                     [
                       {
                         "body" => request,
                         "to" => ["localhost"],
                       },
                       {
                         "to"   => "localhost:23003/output",
                         "type" => "watch.publish",
                       },
                     ],
                   ],
                   @messenger.messages)
    end

    def test_feed_not_match
      request = {
        "targets" => {
          "text" => "たこやきおいしいです"
        }
      }
      assert_nil(process(:feed, request))
    end

    private
    def setup_subscription
      request = {
        "route" => "localhost:23003/output",
        "condition" => "たいやき",
        "subscriber" => "localhost"
      }
      message = Droonga::Test::StubHandlerMessage.new(request, {})
      subscribe_handler =
        Droonga::Plugins::Watch::SubscribeHandler.new("droonga",
                                                      @handler.context,
                                                      @messenger,
                                                      @loop)
      response = subscribe_handler.handle(message)
      assert_equal(SUCCESS_RESULT, response)
    end
  end
end
