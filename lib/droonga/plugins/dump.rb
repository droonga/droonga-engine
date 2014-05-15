# Copyright (C) 2014 Droonga Project
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

require "groonga"

require "droonga/plugin"
require "droonga/error_messages"

module Droonga
  module Plugins
    module Dump
      extend Plugin
      register("dump")

      class Handler < Droonga::Handler
        def handle(message)
          request = Request.new(message)
          if request.need_dump?
            dumper = Dumper.new(@context, loop, messenger, request)
            dumper.start_dump
            true
          else
            false
          end
        end
      end

      class Request
        def initialize(message)
          @message = message
        end

        def need_dump?
          reply_to
        end

        def id
          @message["id"]
        end

        def dataset
          @message.raw["dataset"]
        end

        def reply_to
          (@message.raw["replyTo"] || {})["to"]
        end

        def messages_per_seconds
          request = (@message.request || {})
          minimum_messages_per_seconds = 10
          [
            minimum_messages_per_seconds,
            (request["messagesPerSecond"] || 10000).to_i,
          ].max
        end
      end

      class Dumper
        def initialize(context, loop, messenger, request)
          @context = context
          @loop = loop
          @messenger = messenger
          @request = request
        end

        def start_dump
          setup_forward_data

          forward("dump.start")

          dumper = Fiber.new do
            dump_records
            forward("dump.end")
          end

          timer = Coolio::TimerWatcher.new(0.1, true)
          timer.on_timer do
            begin
              dumper.resume
            rescue FiberError
              timer.detach
            end
          end

          @loop.attach(timer)
        end

        private
        def setup_forward_data
          @base_forward_message = {
            "inReplyTo" => @request.id,
            "dataset"   => @request.dataset,
          }
          @forward_to = @request.reply_to
          @n_forwarded_messages = 0
          @messages_per_100msec = @request.messages_per_seconds / 10
        end

        def forward(type, body=nil)
          forward_message = @base_forward_message
          if body
            forward_message = forward_message.merge("body" => body)
          end
          @messenger.forward(forward_message,
                             "to"   => @forward_to,
                             "type" => type)

          @n_forwarded_messages += 1
          @n_forwarded_messages %= @messages_per_100msec
          Fiber.yield if @n_forwarded_messages.zero?
        end

        def dump_records
          each_table do |table|
            table.each do |record|
              values = {}
              record.attributes.each do |key, value|
                values[key] = value unless key.start_with?("_")
              end
              body = {
                "table" => table.name,
                "key" => record.key,
                "values" => values,
              }
              forward("dump.record", body)
            end
          end
        end

        def each_table
          @context.database.each(:ignore_missing_object => true) do |object|
            next unless object.is_a?(::Groonga::Table)
            next if index_only_table?(object)
            yield(object)
          end
        end

        def index_only_table?(table)
          table.columns.all? do |column|
            column.is_a?(::Groonga::IndexColumn)
          end
        end
      end

      define_single_step do |step|
        step.name = "dump"
        step.handler = Handler
        step.collector = Collectors::And
      end
    end
  end
end
