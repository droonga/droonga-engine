# Copyright (C) 2013 Droonga Project
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
  class GroongaInputAdapter
    class Select
      def convert(select_request)
        table = select_request["table"]
        result_name = table + "_result"
        match_columns = select_request["match_columns"]
        match_to = match_columns ? match_columns.split(/ *\|\| */) : []
        query = select_request["query"]
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
        if query
          condition = {
            "query"  => query,
            "matchTo"=> match_to,
            "defaultOperator"=> "&&",
            "allowPragma"=> false,
            "allowColumn"=> true,
          }
          search_request["queries"][result_name]["condition"] = condition
        end
        search_request
      end
    end
  end
end
