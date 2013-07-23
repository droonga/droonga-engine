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

require "droonga/plugin/adapter/groonga/select"

class AdapterGroongaSelectTest < Test::Unit::TestCase
  def setup
    @select = Droonga::GroongaAdapter::Select.new
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
      @select.convert_request(select_request)
    end

    class OutputColumnsTest < self
      def assert_attributes(expected_attributes, output_columns)
        select_request = {
          "table" => "EmptyTable",
          "output_columns" => output_columns,
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
                "attributes" => expected_attributes,
              },
            },
          },
        }
        assert_equal(expected_search_request, convert(select_request))
      end

      def test_multiple_columns
        assert_attributes(["_id", "_key"], "_id,_key")
      end

      class FunctionTest < self
        def test_single_argument
          assert_attributes(["snippet_html(content)"], "snippet_html(content)")
        end

        def test_with_columns
          assert_attributes(["_id","_key","snippet_html(content)"], "_id,_key,snippet_html(content)")
        end
      end
    end

    class MatchColumnsTest < self
      def assert_matchTo(expected_matchTo, match_columns)
        select_request = {
          "table"          => "EmptyTable",
          "match_columns"  => match_columns,
          "query"          => "QueryTest",
          "output_columns" => "_id",
        }

        expected_search_request = {
          "queries" => {
            "EmptyTable" => {
              "source"   => "EmptyTable",
              "condition"=> {
                "query"  => "QueryTest",
                "matchTo"=> expected_matchTo,
                "defaultOperator"=> "&&",
                "allowPragma"=> false,
                "allowColumn"=> true,
              },
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

      def test_single_column
        assert_matchTo(["_key"], "_key")
      end

      def test_multiple_columns
        assert_matchTo(["_key", "content"], "_key || content")
      end
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
      @select.convert_response(search_response)
    end
  end
end
