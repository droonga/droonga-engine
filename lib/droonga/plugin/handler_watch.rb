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

module Droonga
  class WatchHandler < Droonga::Handler
    Droonga::HandlerPlugin.register("watch", self)

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
  end
end
