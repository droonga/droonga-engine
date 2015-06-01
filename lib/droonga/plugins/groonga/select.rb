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

require "droonga/plugin"

module Droonga
  module Plugins
    module Groonga
      module Select
        DEFAULT_QUERY_FLAGS = "ALLOW_PRAGMA|ALLOW_COLUMN"
        DRILLDOWN_RESULT_PREFIX = "drilldown_result_"

        class RequestConverter
          def convert(select_request)
            @table = select_request["table"]
            @result_name = @table + "_result"

            output_columns = select_request["output_columns"] || "_id, _key, *"
            attributes = convert_output_columns(output_columns)
            offset = (select_request["offset"] || "0").to_i
            limit = (select_request["limit"] || "10").to_i

            output_offset = offset
            output_limit = limit

            sort_by = nil
            sort_keys = (select_request["sortby"] || "").split(",")
            unless sort_keys.empty?
              sort_by = {
                "keys" => sort_keys,
                "offset" => offset,
                "limit" => limit,
              }
              output_offset = 0
            end

            search_request = {
              "queries" => {
                @result_name => {
                  "source" => @table,
                  "output" => {
                    "elements"   => [
                      "startTime",
                      "elapsedTime",
                      "count",
                      "attributes",
                      "records",
                    ],
                    "attributes" => attributes,
                    "offset" => output_offset,
                    "limit" => output_limit,
                  },
                }
              }
            }
            if sort_by
              search_request["queries"][@result_name]["sortBy"] = sort_by
            end

            condition = convert_condition(select_request)
            if condition
              search_request["queries"][@result_name]["condition"] = condition
            end

            drilldown_queries = convert_drilldown(select_request)
            if drilldown_queries
              search_request["queries"].merge!(drilldown_queries)
            end

            search_request
          end

          def convert_condition(select_request)
            match_columns = select_request["match_columns"]
            match_to = match_columns ? match_columns.split(/ *\|\| */) : []
            query = select_request["query"]
            filter = select_request["filter"]

            conditions = []
            if query
              condition = {
                "query"  => query,
                "matchTo"=> match_to,
                "defaultOperator"=> "&&",
              }
              apply_query_flags(condition, select_request["query_flags"])
              conditions << condition
            end

            if filter
              conditions << filter
            end

            condition = nil

            case conditions.size
            when 1
              condition = conditions.first
            when 2
              condition = ["&&"] + conditions
            end

            condition
          end

          def apply_query_flags(condition, flags)
            flags ||= DEFAULT_QUERY_FLAGS
            flags = flags.split("|")
            condition["allowPragma"] = flags.include?("ALLOW_PRAGMA")
            condition["allowColumn"] = flags.include?("ALLOW_COLUMN")
            condition["allowUpdate"] = flags.include?("ALLOW_UPDATE")
            condition["allowLeadingNot"] = flags.include?("ALLOW_LEADING_NOT")
          end

          def convert_drilldown(select_request)
            drilldown_keys = select_request["drilldown"]
            return nil if drilldown_keys.nil? or drilldown_keys.empty?

            drilldown_keys = drilldown_keys.split(",")

            sort_keys = (select_request["drilldown_sortby"] || "").split(",")
            columns   = convert_output_columns(select_request["drilldown_output_columns"] || "_key,_nsubrecs")
            offset    = (select_request["drilldown_offset"] || "0").to_i
            limit     = (select_request["drilldown_limit"] || "10").to_i

            queries = {}
            drilldown_keys.each_with_index do |key, index|
              query = {
                "source" => @result_name,
                "groupBy" => key,
                "output" => {
                  "elements"   => [
                    "count",
                    "attributes",
                    "records",
                  ],
                  "attributes" => columns,
                  "limit" => limit,
                },
              }

              if sort_keys.empty?
                query["output"]["offset"] = offset
              else
                query["sortBy"] = {
                  "keys"   => sort_keys,
                  "offset" => offset,
                  "limit"  => limit,
                }
              end

              queries["#{DRILLDOWN_RESULT_PREFIX}#{key}"] = query
            end
            queries
          end

          # for a backward compatibility for command_version=1,
          # whitespace-separeted case (without functions) should be accepted.
          COMMAND_VERSION_1_ONLY_PATTERN = /\A[^\s,()]+(\s+[^\s,()]+)+\z/

          def convert_output_columns(output_columns)
            output_columns = output_columns.strip
            command_version_is_1 = output_columns =~ COMMAND_VERSION_1_ONLY_PATTERN
            if command_version_is_1
              output_columns.split(/\s+/)
            else
              output_columns.split(/\s*,\s*/)
            end
          end
        end

        class ResponseConverter
          def convert(search_response)
            @drilldown_results = []
            search_response.each do |key, value|
              if key.start_with?(DRILLDOWN_RESULT_PREFIX)
                key = key[DRILLDOWN_RESULT_PREFIX.size..-1]
                convert_drilldown_result(key, value)
              else
                convert_main_result(value)
              end
            end

            select_results = [@body] + @drilldown_results
            [@header, select_results]
          end

          private
          def convert_main_result(result)
            status_code = 0
            start_time = result["startTime"]
            start_time_in_unix_time = normalize_time(start_time).to_f
            elapsed_time = result["elapsedTime"] || 0
            @header = [status_code, start_time_in_unix_time, elapsed_time]
            @body = convert_search_result(result)
          end

          def normalize_time(time)
            time ||= Time.now
            time = Time.parse(time) if time.is_a?(String)
            time
          end

          def convert_drilldown_result(key, result)
            @drilldown_results << convert_search_result(result)
          end

          def convert_search_result(result)
            count      = result["count"]
            attributes = convert_attributes(result["attributes"])
            records    = convert_records(attributes, result["records"] || [])
            [[count], attributes, *records]
          end

          def convert_attributes(attributes)
            attributes = attributes || []
            attributes.collect do |attribute|
              name = attribute["name"]
              type = attribute["type"]
              [name, type]
            end
          end

          def convert_records(attributes, records)
            records.collect do |record|
              record.collect.each_with_index do |value, i|
                name, type = attributes[i]
                _ = name # suppress a warning
                case type
                when "Time"
                  normalize_time(value).to_f
                else
                  value
                end
              end
            end
          end
        end

        class Adapter < Droonga::Adapter
          input_message.pattern = ["type", :equal, "select"]

          def adapt_input(input_message)
            converter = RequestConverter.new
            select_request = input_message.body
            search_request = converter.convert(select_request)
            logger.debug("Conversion of select:",
                         :select => select_request,
                         :search => search_request)
            input_message.type = "search"
            input_message.body = search_request
          end

          def adapt_output(output_message)
            converter = ResponseConverter.new
            search_response = output_message.body
            select_response = converter.convert(search_response)
            output_message.body = select_response
          end
        end
      end
    end
  end
end
