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

require "droonga/plugins/groonga/select"

class GroongaSelectAdapterOutputTest < Test::Unit::TestCase
  private
  def convert(search_response)
    converter = Droonga::Plugins::Groonga::Select::ResponseConverter.new
    converter.convert(search_response)
  end

  class RecordsTest < self
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
      expected_select_response = [
        [status_code, start_time_in_unix_time, elapsed_time],
        [
          [[count], headers],
        ],
      ]

      assert_equal(expected_select_response, convert(search_response))
    end
  end

  class DrilldownTest < self
    START_TIME   = "2001-08-02T10:45:23.5+09:00"
    ELAPSED_TIME = 0
    COUNT        = 0

    def main_search_result
      {
        "startTime"   => START_TIME,
        "elapsedTime" => ELAPSED_TIME,
        "count"       => COUNT,
        "attributes"  => [
          {"name" => "_id", "type" => "UInt32", "vector" => false},
        ],
        "records"     => [],
      }
    end

    def expected_header
      status_code = 0
      start_time_in_unix_time = Time.parse(START_TIME).to_f
      [status_code, start_time_in_unix_time, ELAPSED_TIME]
    end

    def expected_main_select_result
      headers = [["_id","UInt32"]]
      [[COUNT], headers]
    end

    def test_no_drilldown
      search_response = {
        "EmptyTable" => main_search_result,
      }

      expected_select_response = [
        expected_header,
        [
          expected_main_select_result,
        ],
      ]

      assert_equal(expected_select_response, convert(search_response))
    end

    def test_have_one_result
      search_response = {
        "EmptyTable" => main_search_result,
        "drilldown_result_a" => {
          "count" => 3,
          "attributes" => [
            {"name" => "_id", "type" => "UInt32", "vector" => false},
            {"name" => "_key", "type" => "ShortText", "vector" => false},
            {"name" => "_nsubrecs", "type" => "UInt32", "vector" => false},
          ],
          "records" => [
            [1, "a1", 10],
            [2, "a2", 20],
            [3, "a3", 30],
          ],
        },
      }

      headers = [
        ["_id", "UInt32"],
        ["_key", "ShortText"],
        ["_nsubrecs", "UInt32"],
      ]
      expected_select_response = [
        expected_header,
        [
          expected_main_select_result,
          [
            [3],
            headers,
            [
              [1, "a1", 10],
              [2, "a2", 20],
              [3, "a3", 30],
            ],
          ],
        ],
      ]

      assert_equal(expected_select_response, convert(search_response))
    end

    def test_have_multiple_results
      search_response = {
        "EmptyTable" => main_search_result,
        "drilldown_result_a" => {
          "count" => 3,
          "attributes" => [
            {"name" => "_key", "type" => "ShortText", "vector" => false},
            {"name" => "_nsubrecs", "type" => "UInt32", "vector" => false},
          ],
          "records" => [
            ["a1", 10],
            ["a2", 20],
            ["a3", 30],
          ],
        },
        "drilldown_result_b" => {
          "count" => 2,
          "attributes" => [
            {"name" => "_key", "type" => "ShortText", "vector" => false},
            {"name" => "_nsubrecs", "type" => "UInt32", "vector" => false},
          ],
          "records" => [
            ["b1", 10],
            ["b2", 20],
          ],
        },
      }

      headers = [
        ["_key", "ShortText"],
        ["_nsubrecs", "UInt32"],
      ]
      expected_select_response = [
        expected_header,
        [
          expected_main_select_result,
          [
            [3],
            headers,
            [
              ["a1", 10],
              ["a2", 20],
              ["a3", 30],
            ],
          ],
          [
            [2],
            headers,
            [
              ["b1", 10],
              ["b2", 20],
            ],
          ],
        ],
      ]

      assert_equal(expected_select_response, convert(search_response))
    end
  end
end
