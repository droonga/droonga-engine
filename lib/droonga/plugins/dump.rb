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

      class Handler < AsyncCommand::Handler
        private
        def start(request)
          dumper = Dumper.new(@context, loop, messenger, request)
          dumper.start
        end
      end

      class Dumper < AsyncCommand::AsyncHandler
        def initialize(context, loop, messenger, request)
          @context = context
          super(loop, messenger, request)
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

        def handle
          dump_schema
          dump_records
          dump_indexes
          forward("#{prefix}.end")
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
          return false if table.columns.empty?
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
