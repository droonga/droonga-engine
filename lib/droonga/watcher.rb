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

module Droonga
  class Watcher
    EXACT_MATCH = false

    def initialize(context)
      @context = context
    end

    def subscribe(request)
      subscriber = request[:subscriber]
      condition  = request[:condition]
      query      = request[:query]
      route      = request[:route]

      query_table = @context["Query"]
      query_record = query_table[query]
      unless query_record
        keywords = pick_keywords([], condition)
        query_record = query_table.add(query, :keywords => keywords)
      end
      subscriber_table = @context["Subscriber"]
      subscriber_record = subscriber_table[subscriber]
      if subscriber_record
        subscriptions = subscriber_record.subscriptions.collect do |query|
          return if query == query_record
          query
        end
        subscriptions << query_record
        subscriber_record.subscriptions = subscriptions
      else
        subscriber_table.add(subscriber,
                             :subscriptions => [query_record],
                             :route => route,
                             :last_modified => Time.now)
      end
    end

    def unsubscribe(request)
      subscriber = request[:subscriber]
      condition  = request[:condition]
      query      = request[:query]

      query_table = @context["Query"]
      query_record = query_table[query]
      return unless query_record
      subscriber_table = @context["Subscriber"]
      subscriber_record = subscriber_table[subscriber]
      return unless subscriber_record
      subscriptions = subscriber_record.subscriptions.select do |query|
        query != query_record
      end
      subscriber_record.subscriptions = subscriptions
    end

    def feed(request, &block)
      targets = request[:targets]

      hits = []
      targets.each do |key, target|
        scan_body(hits, target)
      end

      publish(hits, request, &block)
    end

    def pick_keywords(memo, condition)
      case condition
      when Hash
        memo << condition["query"]
      when String
        memo << condition
      when Array
        condition[1..-1].each do |element|
          pick_keywords(memo, element)
        end
      end
      memo
    end

    def scan_body(hits, body)
      trimmed = body.strip
      candidates = {}
      # FIXME scan reports the longest keyword matched only
      @context["Keyword"].scan(trimmed).each do |keyword, word, start, length|
        @context["Query"].select do |query|
          query.keywords =~ keyword
        end.each do |record|
          candidates[record.key] ||= []
          candidates[record.key] << keyword
        end
      end
      candidates.each do |query, keywords|
        hits << query if query_match(query, keywords)
      end
    end

    def query_match(query, keywords)
      return true unless EXACT_MATCH
      @conditions = {} unless @conditions
      condition = @conditions[query.id]
      unless condition
        condition = JSON.parse(query.key)
        @conditions[query.id] = condition
        # CAUTION: @conditions can be huge.
      end
      words = {}
      keywords.each do |keyword|
        words[keyword.key] = true
      end
      eval_condition(condition, words)
    end

    def eval_condition(condition, words)
      case condition
      when Hash
        # todo
      when String
        words[condition]
      when Array
        case condition.first
        when "||"
          condition[1..-1].each do |element|
            return true if eval_condition(element, words)
          end
          false
        when "&&"
          condition[1..-1].each do |element|
            return false unless eval_condition(element, words)
          end
          true
        when "-"
          return false unless eval_condition(condition[1], words)
          condition[2..-1].each do |element|
            return false if eval_condition(element, words)
          end
          true
        end
      end
    end

    def publish(hits, request)
      routes = {}
      hits.each do |query|
        @context["Subscriber"].select do |subscriber|
          subscriber.subscriptions =~ query
        end.each do |subscriber|
          routes[subscriber.route.key] ||= []
          routes[subscriber.route.key] << subscriber.key.key
        end
      end
      routes.each do |route, subscribers|
        yield(route, subscribers)
      end
    end
  end
end
