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

require "droonga/handler"

# TODO User -> Subscriber

module Droonga
  class WatchHandler < Droonga::Handler
    Droonga::HandlerPlugin.register("watch", self)
    EXACT_MATCH = false

    command "watch"
    def watch(request)
      user, condition, query, route = parse_request(request)
      query_table = @context['Query']
      query_record = query_table[query]
      unless query_record
        keywords = pick_keywords([], condition)
        query_record = query_table.add(query, :keywords => keywords)
      end
      user_table = @context['User']
      user_record = user_table[user]
      if user_record
        subscriptions = user_record.subscriptions.collect do |query|
          return if query == query_record
          query
        end
        subscriptions << query_record
        user_record.subscriptions = subscriptions
      else
        user_table.add(user,
                       :subscriptions => [query_record],
                       :route => route)
      end
      # TODO return watch result to client
    end

    # TODO unwatch

    command "feed"
    def feed(request)
      targets = request["targets"]

      hits = []
      targets.each do |key, target|
        scan_body(hits, target)
      end

      publish(hits, request)
    end

    private
    def parse_request(request)
      user = request["user"]
      condition = request["condition"]
      route = request["route"]
      raise "invalid request" if user.nil? || user.empty? || condition.nil?
      query = condition.to_json
      raise "too long query" if query.size > 4095
      [user, condition, query, route]
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
      @context['Keyword'].scan(trimmed).each do |keyword, word, start, length|
        @context['Query'].select do |query|
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
        @context['User'].select do |user|
          user.subscriptions =~ query
        end.each do |user|
          routes[user.route.key] ||= []
          routes[user.route.key] << user.key.key
        end
      end
      routes.each do |route, users|
        message = request
        envelope["to"] = users
        post(message, "to" => route, "type" => "watch.notification")
      end
    end
  end
end
