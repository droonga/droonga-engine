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

require "droonga/watcher"
require "droonga/sweeper"

class SqeeperTest < Test::Unit::TestCase
  include WatchHelper

  def setup
    setup_database
    setup_schema
    @context = Groonga::Context.default
    @watcher = Droonga::Watcher.new(@context)
    @sweeper = Droonga::Sweeper.new(@context)
  end

  def teardown
    @watcher = nil
    @sweeper = nil
    teardown_database
  end

  private
  def subscriber_table
    @context["Subscriber"]
  end

  def existing_subscribers
    subscriber_table.select.collect(&:_key)
  end

  public
  class SweepSubscribersTest < self
    NINE_MINUTES_IN_SECONDS   = 9 * 60
    TEN_MINUTES_IN_SECONDS    = 10 * 60
    ELEVEN_MINUTES_IN_SECONDS = 11 * 60

    def setup
      super
      @now = Time.now
      setup_expired_subscribers
    end

    def test_single_term
      @sweeper.sweep_expired_subscribers(:now => @now)
      assert_equal(
        ["subscriber1", "subscriber2"],
        existing_subscribers
      )
    end

    private
    def setup_expired_subscribers
      request1 = {
        :route => "localhost:23003/output",
        :condition => "たいやき",
        :query => "たいやき".to_json,
        :subscriber => "subscriber1",
      }
      @watcher.subscribe(request1)
      request2 = {
        :route => "localhost:23003/output",
        :condition => "たいやき",
        :query => "たいやき".to_json,
        :subscriber => "subscriber2",
      }
      @watcher.subscribe(request2)
      request3 = {
        :route => "localhost:23003/output",
        :condition => "たいやき",
        :query => "たいやき".to_json,
        :subscriber => "subscriber3",
      }
      @watcher.subscribe(request3)
      subscriber_table["subscriber1"].last_modified = @now - NINE_MINUTES_IN_SECONDS
      subscriber_table["subscriber2"].last_modified = @now - TEN_MINUTES_IN_SECONDS
      subscriber_table["subscriber3"].last_modified = @now - ELEVEN_MINUTES_IN_SECONDS
    end
  end
end
