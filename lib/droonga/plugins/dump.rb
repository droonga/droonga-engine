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
require "droonga/error_messages"

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
        include Loggable

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
            dump_schema
            dump_records
            dump_indexes
            forward("dump.end")
          end

          on_error = lambda do |exception|
            message = "failed to dump"
            logger.exception(message, $!)
            error("DumpFailure", message)
          end

          timer = Coolio::TimerWatcher.new(0.1, true)
          timer.on_timer do
            begin
              dumper.resume
            rescue FiberError
              timer.detach
            rescue
              timer.detach
              on_error.call($!)
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

        def error(name, message)
          message = {
            "statusCode" => ErrorMessages::InternalServerError::STATUS_CODE,
            "body" => {
              "name"    => name,
              "message" => message,
            },
          }
          error_message = @base_forward_message.merge(message)
          @messenger.forward(error_message,
                             "to"   => @forward_to,
                             "type" => "dump.error")
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

        def dump_schema
          reference_tables = []
          each_table do |table|
            if reference_table?(table)
              reference_tables << table
              next
            end
            dump_table(table)
          end

          reference_tables.each do |table|
            dump_table(table)
          end
        end

        def dump_table(table)
          forward("dump.table", table_body(table))

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
          forward("dump.column", column_body(column))
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
              forward("dump.record", body)
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
          else
            value
          end
        end

        def dump_indexes
          each_index_columns do |column|
            dump_column(column)
          end
        end

        def each_table
          options = {
            :ignore_missing_object => true,
            :order_by => :key,
          }
          @context.database.each(options) do |object|
            next unless table?(object)
            yield(object)
          end
        end

        def table?(object)
          object.is_a?(::Groonga::Table)
        end

        def index_only_table?(table)
          table.columns.all? do |column|
            index_column?(column)
          end
        end

        def reference_table?(table)
          table.support_key? and table?(table.domain)
        end

        def index_column?(column)
          column.is_a?(::Groonga::IndexColumn)
        end

        def each_index_columns
          each_table do |table|
            table.columns.each do |column|
              yield(column) if index_column?(column)
            end
          end
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
