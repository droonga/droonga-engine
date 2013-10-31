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
require "droonga/handler"

module Droonga
  class WatchHandler < Droonga::Handler
    Droonga::HandlerPlugin.register("watch", self)

    def initialize(*args)
      super
      @watcher = Watcher.new(@context)
    end

    command "watch.subscribe" => :subscribe
    def subscribe(request)
      subscriber, condition, query, route = parse_request(request)
      normalized_request = {
        :subscriber => subscriber,
        :condition  => condition,
        :query      => query,
        :route      => route,
      }
      @watcher.subscribe(normalized_request)
      emit([true])
    end

    command "watch.unsubscribe" => :unsubscribe
    def unsubscribe(request)
      subscriber, condition, query, route = parse_request(request)
      normalized_request = {
        :subscriber => subscriber,
        :condition  => condition,
        :query      => query,
      }
      @watcher.unsubscribe(normalized_request)
      emit([true])
    end

    command "watch.feed" => :feed
    def feed(request)
      @watcher.feed(:targets => request["targets"]) do |route, subscribers|
        message = request # return request itself
        envelope["to"] = subscribers
        post(message, "to" => route, "type" => "watch.notification")
      end
    end

    private
    def parse_request(request)
      subscriber = request["subscriber"]
      condition = request["condition"]
      route = request["route"] || envelope["from"]
      raise "invalid request" if subscriber.nil? || subscriber.empty? || condition.nil?
      query = condition.to_json
      [subscriber, condition, query, route]
    end
  end
end
