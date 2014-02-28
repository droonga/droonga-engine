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

require "droonga/plugin"
require "droonga/watcher"
require "droonga/sweeper"
require "droonga/watch_schema"

module Droonga
  module Plugins
    module Watch
      extend Plugin
      register("watch")

      module SchemaCreatable
        private
        def ensure_schema_created
          # XXX just workaround. This must be re-written.
          # When secondary and later processes opens the database,
          # creation processes of tables by the first process is
          # not finished yet. Then secondary and others tries to
          # create tables and raises errors. To avoid such a problem,
          # the creation processes of tables is disabled on workers.
          if $0 !~ /\AServer/
            schema = WatchSchema.new(@context)
            schema.ensure_created
          else
            until @context["Keyword"]
              sleep 0.1
            end
            sleep 1
          end
        end
      end

      module MessageParsable
        private
        def parse_message(message)
          request = message.request
          subscriber = request["subscriber"]
          condition = request["condition"]
          route = request["route"] || message["from"]
          if condition
            query = condition.to_json
          else
            query = nilondition
          end
          [subscriber, condition, query, route]
        end
      end

      class SubscribeHandler < Droonga::Handler
        include SchemaCreatable
        include MessageParsable

        def initialize(*args)
          super
          ensure_schema_created # TODO: REMOVE ME
        end

        def handle(message)
          subscriber, condition, query, route = parse_message(message)
          normalized_request = {
            :subscriber => subscriber,
            :condition  => condition,
            :query      => query,
            :route      => route,
          }
          watcher = Watcher.new(@context)
          watcher.subscribe(normalized_request)
          true
        end
      end

      define_single_step do |step|
        step.name = "watch.subscribe"
        step.write = true
        step.handler = SubscribeHandler
        step.collector = Collectors::And
      end

      class UnsubscribeHandler < Droonga::Handler
        include SchemaCreatable
        include MessageParsable

        def initialize(*args)
          super
          ensure_schema_created # TODO: REMOVE ME
        end

        def handle(message)
          subscriber, condition, query, route = parse_message(message)
          normalized_request = {
            :subscriber => subscriber,
            :condition  => condition,
            :query      => query,
          }
          watcher = Watcher.new(@context)
          watcher.unsubscribe(normalized_request)
          true
        end
      end

      define_single_step do |step|
        step.name = "watch.unsubscribe"
        step.write = true
        step.handler = UnsubscribeHandler
        step.collector = Collectors::And
      end

      class FeedHandler < Droonga::Handler
        include SchemaCreatable

        def initialize(*args)
          super
          ensure_schema_created # TODO: REMOVE ME
        end

        def handle(message)
          request = message.request
          watcher = Watcher.new(@context)
          watcher.feed(:targets => request["targets"]) do |route, subscribers|
            published_message = {
              "to"   => subscribers,
              "body" => request,
            }
            published_message = message.raw.merge(published_message)
            messenger.forward(published_message,
                              "to" => route, "type" => "watch.publish")
          end
          nil
        end
      end

      define_single_step do |step|
        step.name = "watch.feed"
        step.write = true
        step.handler = FeedHandler
      end

      class SweepHandler < Droonga::Handler
        include SchemaCreatable

        message.type = "watch.sweep"

        def initialize(*args)
          super
          ensure_schema_created # TODO: REMOVE ME
        end

        def handle(message)
          sweeper = Sweeper.new(@context)
          sweeper.sweep_expired_subscribers
          nil
        end
      end

      define_single_step do |step|
        step.name = "watch.sweep"
        step.write = true
        step.handler = SweepHandler
      end
    end
  end
end
