# -*- coding: utf-8 -*-
#
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

require "droonga/watcher"

class WatcherTest < Test::Unit::TestCase
  include WatchHelper

  def setup
    setup_database
    setup_schema
    setup_watcher
  end

  def teardown
    teardown_watcher
    teardown_database
  end

  private
  def setup_watcher
    @context = Groonga::Context.default
    @watcher = Droonga::Watcher.new(@context)
  end

  def teardown_watcher
    @watcher = nil
  end

  def subscriber_table
    @context["Subscriber"]
  end

  def query_table
    @context["Query"]
  end

  def keyword_table
    @context["Keyword"]
  end

  def existing_subscribers
    subscriber_table.select.collect(&:_key)
  end

  def existing_queries
    query_table.select.collect(&:_key)
  end

  def existing_keywords
    keyword_table.select.collect(&:_key)
  end

  def existing_records
    {
      :subscribers => existing_subscribers,
      :queries => existing_queries,
      :keywords => existing_keywords,
     }
  end

  def normalize_subscriber(subscriber)
    return nil if subscriber.nil?
    {
      :_key => subscriber._key,
      :subscriptions => subscriber.subscriptions.collect(&:_key),
      :route => subscriber.route._key,
    }
  end

  def normalize_queries(queries)
    return nil if queries.nil?
    queries.collect do |query|
      normalize_query(query)
    end
  end

  def normalize_query(query)
    return nil if query.nil?
    {
      :_key => query._key,
      :keywords => query.keywords.collect(&:_key),
    }
  end

  public
  class SubscribeTest < self
    def test_single_term
      request = {
        :route => "localhost:23003/output",
        :condition => "たいやき",
        :query => "たいやき".to_json,
        :subscriber => "localhost",
      }
      @watcher.subscribe(request)

      assert_equal(
        {:subscribers => ["localhost"],
         :queries => ["たいやき".to_json],
         :keywords => ["たいやき"]},
        existing_records
      )
      assert_equal(
        {:_key => "localhost",
         :subscriptions => ["たいやき".to_json],
         :route => "localhost:23003/output"},
        normalize_subscriber(subscriber_table.first)
      )
      assert_equal(
        [{:_key => "たいやき".to_json, :keywords => ["たいやき"]}],
        normalize_queries(subscriber_table.first.subscriptions)
      )
    end

=begin
# this test will be activated when condition with multiple tabs is supproted.
    def test_multiple_terms
      request = {
        :route => "localhost:23003/output",
        :condition => "たいやき たこやき",
        :query => "たいやき たこやき".to_json,
        :subscriber => "localhost",
      }
      @watcher.subscribe(request)

      assert_equal(["localhost"], existing_subscribers)
      assert_equal(
        {:_key => "localhost",
         :subscriptions => ["たいやき たこやき".to_json],
         :route => "localhost:23003/output"},
        normalize_subscriber(subscriber_table.first)
      )
      assert_equal(["たいやき たこやき".to_json], existing_queries)
      assert_equal(["たいやき", "たこやき"], existing_keywords)
    end
=end

    def test_same_condition_multiple_times
      request = {
        :route => "localhost:23003/output",
        :condition => "たいやき",
        :query => "たいやき".to_json,
        :subscriber => "localhost",
      }
      @watcher.subscribe(request)
      @watcher.subscribe(request)
      assert_equal(
        {:subscribers => ["localhost"],
         :queries => ["たいやき".to_json],
         :keywords => ["たいやき"]},
        existing_records
      )
    end

    def test_different_conditions
      request1 = {
        :route => "localhost:23003/output",
        :condition => "たいやき",
        :query => "たいやき".to_json,
        :subscriber => "localhost",
      }
      @watcher.subscribe(request1)
      request2 = {
        :route => "localhost:23003/output",
        :condition => "たこやき",
        :query => "たこやき".to_json,
        :subscriber => "localhost",
      }
      @watcher.subscribe(request2)
      assert_equal(
        {:subscribers => ["localhost"],
         :queries => ["たいやき".to_json, "たこやき".to_json],
         :keywords => ["たいやき", "たこやき"]},
        existing_records
      )
      assert_equal(
        {:_key => "localhost",
         :subscriptions => ["たいやき".to_json, "たこやき".to_json],
         :route => "localhost:23003/output"},
        normalize_subscriber(subscriber_table.first)
      )
    end

    def test_multiple_subscribers
      request = {
        :route => "localhost:23003/output",
        :condition => "たいやき",
        :query => "たいやき".to_json,
        :subscriber => "subscriber1",
      }
      @watcher.subscribe(request)
      request = {
        :route => "localhost:23003/output",
        :condition => "たこやき",
        :query => "たこやき".to_json,
        :subscriber => "subscriber2",
      }
      @watcher.subscribe(request)
      assert_equal(
        {:subscribers => ["subscriber1", "subscriber2"],
         :queries => ["たいやき".to_json, "たこやき".to_json],
         :keywords => ["たいやき", "たこやき"]},
        existing_records
      )
      assert_equal([["たいやき".to_json],
                    ["たこやき".to_json]],
                   [subscriber_table["subscriber1"].subscriptions.collect(&:_key),
                    subscriber_table["subscriber2"].subscriptions.collect(&:_key)])
    end
  end

  class UnsubscribeTest < self
    def setup
      super
      setup_subscriptions
    end

    def test_with_query_multiple_times
      @watcher.unsubscribe(
        :route => "localhost:23003/output",
        :query => "たいやき".to_json,
        :subscriber => "subscriber1",
      )
      assert_equal(
        {:subscribers => ["subscriber1", "subscriber2"],
         :queries => ["たいやき".to_json, "たこやき".to_json],
         :keywords => ["たいやき", "たこやき"]},
        existing_records
      )

      @watcher.unsubscribe(
        :route => "localhost:23003/output",
        :query => "たこやき".to_json,
        :subscriber => "subscriber1",
      )
      assert_equal(
        {:subscribers => ["subscriber2"],
         :queries => ["たいやき".to_json, "たこやき".to_json],
         :keywords => ["たいやき", "たこやき"]},
        existing_records
      )
    end

    def test_with_query_watched_by_multiple_subscribers
      @watcher.unsubscribe(
        :route => "localhost:23003/output",
        :query => "たいやき".to_json,
        :subscriber => "subscriber1",
      )
      assert_equal(
        {:subscribers => ["subscriber1", "subscriber2"],
         :queries => ["たいやき".to_json, "たこやき".to_json],
         :keywords => ["たいやき", "たこやき"]},
        existing_records
      )

      @watcher.unsubscribe(
        :route => "localhost:23003/output",
        :query => "たいやき".to_json,
        :subscriber => "subscriber2",
      )
      assert_equal(
        {:subscribers => ["subscriber1", "subscriber2"],
         :queries => ["たこやき".to_json],
         :keywords => ["たこやき"]},
        existing_records
      )
    end

    def test_without_query
      request = {
        :route => "localhost:23003/output",
        :subscriber => "subscriber1",
      }
      @watcher.unsubscribe(request)
      assert_equal(
        {:subscribers => ["subscriber2"],
         :queries => ["たいやき".to_json, "たこやき".to_json],
         :keywords => ["たいやき", "たこやき"]},
        existing_records
      )
    end

    private
    def setup_subscriptions
      request1_1 = {
        :route => "localhost:23003/output",
        :condition => "たいやき",
        :query => "たいやき".to_json,
        :subscriber => "subscriber1",
      }
      @watcher.subscribe(request1_1)
      request1_2 = {
        :route => "localhost:23003/output",
        :condition => "たこやき",
        :query => "たこやき".to_json,
        :subscriber => "subscriber1",
      }
      @watcher.subscribe(request1_2)
      request2_1 = {
        :route => "localhost:23003/output",
        :condition => "たいやき",
        :query => "たいやき".to_json,
        :subscriber => "subscriber2",
      }
      @watcher.subscribe(request2_1)
      request2_2 = {
        :route => "localhost:23003/output",
        :condition => "たこやき",
        :query => "たこやき".to_json,
        :subscriber => "subscriber2",
      }
      @watcher.subscribe(request2_2)
      assert_equal(
        {:subscribers => ["subscriber1", "subscriber2"],
         :queries => ["たいやき".to_json, "たこやき".to_json],
         :keywords => ["たいやき", "たこやき"]},
        existing_records
      )
    end
  end
end
