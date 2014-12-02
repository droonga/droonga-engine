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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "groonga"

require "droonga/plugin"
require "droonga/error_messages"

module Droonga
  module Plugins
    module CRUD
      extend Plugin
      register("crud")

      class Adapter < Droonga::Adapter
        input_message.pattern  = ["type", :equal, "add"]
        output_message.pattern = ["body.success", :exist]

        def adapt_input(input_message)
          request = input_message.body
          key = request["key"] || rand.to_s
          values = request["values"] || {}
          request["filter"] = values.merge("_key" => key)
        end

        def adapt_output(output_message)
          if output_message.errors
            detail = output_message.body["detail"]
            return if detail.nil?
            detail.delete("filter")
            output_message.errors.each do |path, error|
              error["body"]["detail"].delete("filter")
            end
          else
            output_message.body.delete("filter")
          end
        end
      end

      class Handler < Droonga::Handler
        class MissingTableParameter < BadRequest
          def initialize
            super("\"table\" must be specified.")
          end
        end

        class MissingPrimaryKeyParameter < BadRequest
          def initialize(table_name)
            super("\"key\" must be specified. " +
                    "The table #{table_name.inspect} requires a primary key for a new record.")
          end
        end

        class UnknownTable < NotFound
          def initialize(table_name)
            super("The table #{table_name.inspect} does not exist in the dataset.")
          end
        end

        class InvalidValue < BadRequest
          def initialize(column, value, request)
            super("The column #{column.inspect} cannot store the value #{value.inspect}.",
                  request)
          end
        end

        class UnknownColumn < NotFound
          def initialize(column, table, request)
            super("The column #{column.inspect} does not exist in the table #{table.inspect}.",
                  request)
          end
        end

        def handle(message)
          process_add(message.request)
        end

        private
        def process_add(request)
          raise MissingTableParameter.new unless request.include?("table")

          table = @context[request["table"]]
          raise UnknownTable.new(request["table"]) unless table

          if table.support_key?
            unless request.include?("key")
              raise MissingPrimaryKeyParameter.new(request["table"])
            end
          end

          add_record(table, request)
          true
        end

        def add_record(table, request)
          record = nil
          if table.support_key?
            key = normalize_record_key(request["key"], table)
            record = table.add(key)
          else
            record = table.add
          end
          (request["values"] || []).each do |column, value|
            begin
              record[column] = value
            rescue ::Groonga::InvalidArgument => error
              record.delete if record.added?
              raise InvalidValue.new(column, value, request)
            rescue ArgumentError => error
              record.delete if record.added?
              raise InvalidValue.new(column, value, request)
            rescue ::Groonga::NoSuchColumn => error
              record.delete if record.added?
              raise UnknownColumn.new(column, request["table"], request)
            end
          end
        end

        def normalize_record_key(key, table)
          case table.domain.name
          when "Int8",  "UInt8",
               "Int16", "UInt16",
               "Int32", "UInt32",
               "Int64", "UInt64"
            key.to_i
          else
            key.to_s
          end
        end
      end

      define_single_step do |step|
        step.name = "add"
        step.inputs = {
          "table" => {
            :type => :table,
            :filter => "filter",
          },
        }
        step.write = true
        step.handler = Handler
        step.collector = Collectors::And
      end
    end
  end
end
