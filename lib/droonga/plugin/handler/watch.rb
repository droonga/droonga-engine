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
require "droonga/sweeper"
require "droonga/watch_schema"
require "droonga/handler_plugin"

module Droonga
  class WatchHandler < Droonga::HandlerPlugin
    repository.register("watch", self)

    def initialize(*args)
      super

      # XXX just workaround. This must be re-written.
      # When secondary and later processes opens the database,
      # creation processes of tables by the first process is
      # not finished yet. Then secondary and others tries to
      # create tables and raises errors. To avoid such a problem,
      # the creation processes of tables is disabled on workers.
      if $0 !~ /\AServer/
        ensure_schema_created
      else
        until @context["Keyword"]
          sleep 0.1
        end
        sleep 1
      end

      @watcher = Watcher.new(@context)
      @sweeper = Sweeper.new(@context)
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
        forward(message, "to" => route, "type" => "watch.notification")
      end
    end

    command "watch.sweep" => :sweep
    def sweep(request)
      @sweeper.sweep_expired_subscribers
    end

    private
    def parse_request(request)
      subscriber = request["subscriber"]
      condition = request["condition"]
      route = request["route"] || envelope["from"]
      query = condition && condition.to_json
      [subscriber, condition, query, route]
    end

    def ensure_schema_created
      schema = WatchSchema.new(@context)
      schema.ensure_created
    end
  end
end
