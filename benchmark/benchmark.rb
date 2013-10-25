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

require "droonga/client"

class Benchmark
  class Terms
    class << self
      def generate
        new.to_enum(:each)
      end
    end

    FIRST_INITIAL_LETTER = "㐀"
    SUFFIX = "あいうえおかきくけこ"
    def each
      initial_letter = FIRST_INITIAL_LETTER
      while true do
        yield "#{initial_letter}#{SUFFIX}"
        initial_letter.succ!
      end
    end
  end

  TERMS_STEP = 1000

  N_TARGETS         = 1000
  TARGET_PADDING    = "パディング"
  TARGET_TITLE_SIZE = 100
  TARGET_BODY_SIZE  = 1000

  def initialize(params)
    @params = params
    @terms = Terms.generate
    @client = Droonga::Client.new(tag: @params[:tag], port: @params[:port])
  end

  def run
    prepare_watching_subscribers(TERMS_STEP)
    populate_feeds(0.1)
    start_at = Time.now
    @feeds.each do |feed|
      @client.send(feed)
    end
    end_at = Time.now
  end

  def prepare_watching_subscribers(step)
    step.times do
      term = @terms.next
      add_subscriber(term)
      @watching_terms << term
    end
  end

  def add_subscriber(term)
    subscribe_envelope = envelope_to_subscribe(term)
    @client.connection.send_receive(subscribe_envelope)
  end

  def envelope_to_subscribe(term)
    {
      "id" => Time.now.to_f.to_s,
      "date" => Time.now,
      "statusCode" => 200,
      "type" => "watch.subscribe",
      "body" => {
        "condition" => term,
        "subscriber" => term,
      },
    }
  end

  def populate_feeds(incidence)
    @feeds = []

    n_matched_targets = (N_TARGETS.to_f * incidence).to_i
    n_unmatched_targets = (N_TARGETS - n_matched_targets)

    n_matched_targets.times do
      @feeds << envelope_to_feed(@watching_terms.sample(1))
    end

    n_unmatched_targets.times do
      @feeds << envelope_to_feed(@terms.next)
    end
  end

  def envelope_to_feed(term)
    {
      "id" => Time.now.to_f.to_s,
      "date" => Time.now,
      "statusCode" => 200,
      "type" => "watch.feed",
      "body" => {
        "targets" => {
          "title" => TARGET_PADDING * (TARGET_TITLE_SIZE / TARGET_PADDING.size),
          "body"  => term + (TARGET_PADDING * (TARGET_BODY_SIZE / TARGET_PADDING.size)),
        },
      },
    }
  end
end
