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
          id = message.raw["id"]
          dataset = message.raw["dataset"]
          replyTo = (message.raw["replyTo"] || {})["to"]
          return false unless replyTo

          request = message.request || {}

          dump_start_message = {
            "inReplyTo" => id,
            "dataset" => dataset,
          }
          messenger.forward(dump_start_message,
                            "to" => replyTo,
                            "type" => "dump.start")

          messages_per_seconds = request["messagesPerSecond"] || 10000
          messages_per_seconds = [10, messages_per_seconds.to_i].max
          messages_per_100msec = messages_per_seconds / 10
          dumper = Fiber.new do
            n = 0
            each_table do |table|
              table.each do |record|
                values = {}
                record.attributes.each do |key, value|
                  values[key] = value unless key.start_with?("_")
                end
                dump_message = {
                  "inReplyTo" => id,
                  "dataset" => dataset,
                  "body" => {
                    "table" => table.name,
                    "key" => record.key,
                    "values" => values,
                  },
                }
                messenger.forward(dump_message,
                                  "to" => replyTo,
                                  "type" => "dump.record")
                n = (n + 1) % messages_per_100msec
                Fiber.yield if n.zero?
              end
            end
            dump_end_message = {
              "inReplyTo" => id,
              "dataset" => dataset,
            }
            messenger.forward(dump_end_message,
                              "to" => replyTo,
                              "type" => "dump.end")
          end

          timer = Coolio::TimerWatcher.new(0.1, true)
          timer.on_timer do
            begin
              dumper.resume
            rescue FiberError
              timer.detach
            end
          end
          loop.attach(timer)

          true
        end

        private
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
