# Copyright (C) 2013 droonga project
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

module Droonga
  class GroongaAdapter
    class Select
      def convert_request(select_request)
        table = select_request["table"]
        match_columns = select_request["match_columns"]
        match_to = match_columns ? match_columns.split(/ *\|\| */) : []
        query = select_request["query"]
        output_columns = select_request["output_columns"]
        attributes = output_columns.split(/, */)

        search_request = {
          "queries" => {
            table => {
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
              },
            }
          }
        }
        if query
          condition = {
            "query"  => query,
            "matchTo"=> match_to,
            "defaultOperator"=> "&&",
            "allowPragma"=> false,
            "allowColumn"=> true,
          }
          search_request["queries"][table]["condition"] = condition
        end
        search_request
      end

      def convert_response(search_response)
        select_responses = search_response.collect do |key, value|
          status_code = 0

          start_time = value["startTime"]
          start_time_in_unix_time = Time.parse(start_time).to_f
          elapsed_time = value["elapsedTime"]
          count = value["count"]

          attributes = value["attributes"]
          converted_attributes = attributes.collect do |attribute|
            name = attribute["name"]
            type = attribute["type"]
            [name, type]
          end

          header = [status_code, start_time_in_unix_time, elapsed_time]
          results = [[count], converted_attributes]
          body = [results]

          [header, body]
        end
        select_responses.first
      end
    end
  end
end
