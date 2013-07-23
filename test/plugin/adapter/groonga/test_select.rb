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

require "droonga/plugin/adapter/adapter_select"

class AdapterGroongaSelectTest < Test::Unit::TestCase
  def setup
    @groonga_adapter = Droonga::GroongaAdapter::Select.new
  end

  class RequestTest < self
    def test_empty
      select_request = {
        "table" => "EmptyTable",
        "output_columns" => "_id",
      }

      expected_search_request = {
        "queries" => {
          "EmptyTable" => {
            "source"   => "EmptyTable",
            "output"   => {
              "elements"   => [
                "startTime",
                "elapsedTime",
                "count",
                "attributes",
                "records",
              ],
              "attributes" => ["_id"],
            },
          },
        },
      }

      assert_equal(expected_search_request, convert(select_request))
    end

    private
    def convert(select_request)
      @groonga_adapter.convert_request(select_request)
    end
  end

  class ResponseTest < self
    def test_empty
      start_time = "2001-08-02T10:45:23.5+09:00"
      elapsed_time = 0
      count = 0

      search_response = {
        "EmptyTable" => {
          "startTime"   => start_time,
          "elapsedTime" => elapsed_time,
          "count"       => count,
          "attributes"  => [
            {"name" => "_id", "type" => "UInt32", "vector" => false},
          ],
          "records"     => [],
        },
      }

      status_code = 0
      start_time_in_unix_time = Time.parse(start_time).to_f
      headers = [["_id","UInt32"]]
      expected_select_response = [[status_code, start_time_in_unix_time, elapsed_time],
                                  [[[count], headers]]]

      assert_equal(expected_select_response, convert(search_response))
    end

    private
    def convert(search_response)
      @groonga_adapter.convert_response(search_response)
    end
  end
end
