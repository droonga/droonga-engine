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

module Droonga
  module Plugins
    module Groonga
      module Select
        class RequestConverter
          def convert(select_request)
            table = select_request["table"]
            result_name = table + "_result"
            match_columns = select_request["match_columns"]
            match_to = match_columns ? match_columns.split(/ *\|\| */) : []
            query = select_request["query"]
            filter = select_request["filter"]
            output_columns = select_request["output_columns"] || ""
            attributes = output_columns.split(/, */)
            offset = (select_request["offset"] || "0").to_i
            limit = (select_request["limit"] || "10").to_i

            search_request = {
              "queries" => {
                result_name => {
                  "source" => table,
                  "output" => {
                    "elements"   => [
                      "startTime",
                      "elapsedTime",
                      "count",
                      "attributes",
                      "records",
                    ],
                    "attributes" => attributes,
                    "offset" => offset,
                    "limit" => limit,
                  },
                }
              }
            }

            conditions = []
            if query
              conditions << {
                "query"  => query,
                "matchTo"=> match_to,
                "defaultOperator"=> "&&",
                "allowPragma"=> false,
                "allowColumn"=> true,
              }
            end

            if filter
              conditions << filter
            end

            case conditions.size
            when 1
              condition = conditions.first
            when 2
              condition = ["&&"] + conditions
            end

            if condition
              search_request["queries"][result_name]["condition"] = condition
            end

            search_request
          end
        end

        class ResponseConverter
          def convert(search_response)
            select_responses = search_response.collect do |key, value|
              status_code = 0

              start_time = value["startTime"]
              start_time_in_unix_time = if start_time
                                          Time.parse(start_time).to_f
                                        else
                                          Time.now.to_f
                                        end
              elapsed_time = value["elapsedTime"] || 0
              count = value["count"]

              attributes = value["attributes"] || []
              converted_attributes = attributes.collect do |attribute|
                name = attribute["name"]
                type = attribute["type"]
                [name, type]
              end

              header = [status_code, start_time_in_unix_time, elapsed_time]
              records = value["records"]
              if records.empty?
                results = [[count], converted_attributes]
              else
                results = [[count], converted_attributes, records]
              end
              body = [results]

              [header, body]
            end
            select_responses.first
          end
        end

        class Adapter < Droonga::Adapter
          input_message.pattern = ["type", :equal, "select"]

          def adapt_input(input_message)
            converter = RequestConverter.new
            select_request = input_message.body
            search_request = converter.convert(select_request)
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
