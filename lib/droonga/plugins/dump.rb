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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "groonga"

require "droonga/plugin"
require "droonga/plugin/async_command"
require "droonga/error_messages"
require "droonga/database_scanner"

module Droonga
  module Plugins
    # TODO: Implement it by superstep:
    #  * Schema: Choose one slice in a replica.
    #    Because all slices has the same schema.
    #    (Should we add "select" type to Dataset#copmute_routes for the case?)
    #  * Fact table: Choose all slices in a replica.
    #    Because records for the fact table are distributed in all slices.
    #    (Should we add "gather" type to Dataset#copmute_routes for the case?)
    #  * Other tables: Choose one slice in a replica.
    #    Because all slices has all records for other tables.
    #    (Should we add "select" type to Dataset#copmute_routes for the case?)

    module Dump
      extend Plugin
      register("dump")

      class Request < AsyncCommand::Request
        DEFAULT_MESSAGES_PER_SECOND = 10000

        def messages_per_second
          request = (@message.request || {})
          minimum_messages_per_second = 10
          [
            minimum_messages_per_second,
            (request["messagesPerSecond"] || DEFAULT_MESSAGES_PER_SECOND).to_i,
          ].max
        end
      end

      class Handler < AsyncCommand::Handler
        private
        def request_class
          Request
        end

        def start(request)
          dumper = Dumper.new(@context, loop, messenger, request)
          dumper.start
        end
      end

      class Dumper < AsyncCommand::AsyncHandler
        include DatabaseScanner

        def initialize(context, loop, messenger, request)
          @context = context
          super(loop, messenger, request)
        end

        def start
          on_start

          runner = Fiber.new do
            forecast
            dump_schema
            dump_records
            dump_indexes
            on_finish
          end

          timer = Coolio::TimerWatcher.new(0.1, true)
          timer.on_timer do
            if runner.alive?
              begin
                runner.resume
              rescue
                timer.detach
                # logger.trace("start: watcher detached on unexpected exception",
                #              :watcher => timer)
                logger.exception(error_message, $!)
                error(error_name, error_message)
              end
            else
              timer.detach
              # logger.trace("start: watcher detached on unexpected exception",
              #              :watcher => timer)
            end
          end

          @loop.attach(timer)
          logger.trace("start: new watcher attached",
                       :watcher => timer)
        end

        private
        def prefix
          "dump"
        end

        def error_name
          "DumpFailure"
        end

        def error_message
          "failed to dump"
        end

        def forecast
          forward("#{prefix}.forecast", "nMessages" => total_n_objects)
        end

        def dump_schema
          each_table do |table|
            dump_table(table)
          end
        end

        def dump_table(table)
          forward("#{prefix}.table", table_body(table))

          columns = table.columns.sort_by(&:name)
          columns.each do |column|
            next if index_column?(column)
            dump_column(column)
          end
        end

        def table_body(table)
          body = {
            "type" => table_type(table),
            "name" => table.name,
          }
          if table.support_key?
            body["keyType"] = table.domain.name
          end
          if body["keyType"] == "ShortText"
            if table.default_tokenizer
              body["tokenizer"] = table.default_tokenizer.name
            end
            if table.normalizer
              body["normalizer"] = table.normalizer.name
            end
          end
          body
        end

        def table_type(table)
          table.class.name.split(/::/).last
        end

        def dump_column(column)
          forward("#{prefix}.column", column_body(column))
        end

        def column_body(column)
          body = {
            "table"     => column.domain.name,
            "name"      => column.local_name,
            "type"      => column_type(column),
            "valueType" => column.range.name,
          }
          case body["type"]
          when "Index"
            body["indexOptions"] = {
              "section"  => column.with_section?,
              "weight"   => column.with_weight?,
              "position" => column.with_position?,
              "sources"  => index_column_sources(column),
            }
          when "Vector"
            body["vectorOptions"] = {
              "weight" => column.with_weight?,
            }
          end
          body
        end

        def column_type(column)
          if index_column?(column)
            "Index"
          elsif column.vector?
            "Vector"
          else
            "Scalar"
          end
        end

        def index_column_sources(index_column)
          index_column.sources.collect do |source|
            if source.is_a?(::Groonga::Table)
              "_key"
            else
              source.local_name
            end
          end
        end

        def dump_records
          each_table do |table|
            next if index_only_table?(table)
            table.each do |record|
              values = {}
              record.attributes.each do |key, value|
                next if key.start_with?("_")
                values[key] = normalize_record_value(value)
              end
              body = {
                "table"  => table.name,
                "key"    => record.key,
                "values" => values,
              }
              forward("#{prefix}.record", body)
            end
          end
        end

        def normalize_record_value(value)
          case value
          when Array
            value.collect do |element|
              case element
              when Hash
                element["_key"]
              else
                element
              end
            end
          when Hash
            value["_key"]
          else
            value
          end
        end

        def dump_indexes
          each_index_columns do |column|
            dump_column(column)
          end
        end

        def setup_forward_data
          super
          @n_forwarded_messages = 0
          @messages_per_100msec = @request.messages_per_second / 10
        end

        def forward(type, body=nil)
          super
          @n_forwarded_messages += 1
          @n_forwarded_messages %= @messages_per_100msec
          Fiber.yield if @n_forwarded_messages.zero?
        end

        def log_tag
          "[#{Process.ppid}] dumper"
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
