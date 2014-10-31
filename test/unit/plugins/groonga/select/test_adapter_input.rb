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

require "droonga/plugins/groonga/select"

class GroongaSelectAdapterInputTest < Test::Unit::TestCase
  private
  def convert(select_request)
    converter = Droonga::Plugins::Groonga::Select::RequestConverter.new
    converter.convert(select_request)
  end

  class OutputColumnsTest < self
    def assert_attributes(expected_attributes, output_columns)
      select_request = {
        "table" => "EmptyTable",
        "output_columns" => output_columns,
      }

      expected_search_request = {
        "queries" => {
          "EmptyTable_result" => {
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
              "offset" => 0,
              "limit" => 10,
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

    class CommandVersion1Test < self
      def test_whiet_space_separated
        assert_attributes(["_id","_key"], "_id _key")
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
          "EmptyTable_result" => {
            "source"   => "EmptyTable",
            "condition"=> {
              "query"  => "QueryTest",
              "matchTo"=> expected_matchTo,
              "defaultOperator"=> "&&",
              "allowPragma"=> true,
              "allowColumn"=> true,
              "allowUpdate"=> false,
              "allowLeadingNot" => false,
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
              "offset" => 0,
              "limit" => 10,
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

  class FilterTest < self
    def test_filter
      select_request = {
        "table"          => "EmptyTable",
        "filter"         => "title@'FilterTest'",
        "output_columns" => "_id",
      }

      expected_search_request = {
        "queries" => {
          "EmptyTable_result" => {
            "source"   => "EmptyTable",
            "condition"=> "title@'FilterTest'",
            "output"   => {
              "elements"   => [
                "startTime",
                "elapsedTime",
                "count",
                "attributes",
                "records",
              ],
              "attributes" => ["_id"],
              "offset" => 0,
              "limit" => 10,
            },
          },
        },
      }
      assert_equal(expected_search_request, convert(select_request))
    end
  end

  class CombinedConditionsTest < self
    def test_conditions
      select_request = {
        "table"          => "EmptyTable",
        "match_columns"  => "_key",
        "query"          => "QueryTest",
        "filter"         => "title@'FilterTest'",
        "output_columns" => "_id",
      }

      expected_search_request = {
        "queries" => {
          "EmptyTable_result" => {
            "source"   => "EmptyTable",
            "condition"=> [
              "&&",
              {
                "query"  => "QueryTest",
                "matchTo"=> ["_key"],
                "defaultOperator"=> "&&",
                "allowPragma"=> true,
                "allowColumn"=> true,
                "allowUpdate"=> false,
                "allowLeadingNot" => false,
              },
              "title@'FilterTest'",
            ],
            "output"   => {
              "elements"   => [
                "startTime",
                "elapsedTime",
                "count",
                "attributes",
                "records",
              ],
              "attributes" => ["_id"],
              "offset" => 0,
              "limit" => 10,
            },
          },
        },
      }
      assert_equal(expected_search_request, convert(select_request))
    end
  end

  class QueryFlagsTest < self
    def test_none
      select_request = {
        "table"          => "EmptyTable",
        "match_columns"  => "_key",
        "query"          => "QueryTest",
        "query_flags"    => "NONE",
      }
      expected_search_condition = {
        "query"  => "QueryTest",
        "matchTo"=> ["_key"],
        "defaultOperator"=> "&&",
        "allowPragma" => false,
        "allowColumn" => false,
        "allowUpdate"=> false,
        "allowLeadingNot" => false,
      }
      assert_equal(expected_search_condition,
                   convert(select_request)["queries"]["EmptyTable_result"]["condition"])
    end

    def test_allow_pragma_only
      select_request = {
        "table"          => "EmptyTable",
        "match_columns"  => "_key",
        "query"          => "QueryTest",
        "query_flags"    => "ALLOW_PRAGMA",
      }
      expected_search_condition = {
        "query"  => "QueryTest",
        "matchTo"=> ["_key"],
        "defaultOperator"=> "&&",
        "allowPragma" => true,
        "allowColumn" => false,
        "allowUpdate"=> false,
        "allowLeadingNot" => false,
      }
      assert_equal(expected_search_condition,
                   convert(select_request)["queries"]["EmptyTable_result"]["condition"])
    end

    def test_allow_column_only
      select_request = {
        "table"          => "EmptyTable",
        "match_columns"  => "_key",
        "query"          => "QueryTest",
        "query_flags"    => "ALLOW_COLUMN",
      }
      expected_search_condition = {
        "query"  => "QueryTest",
        "matchTo"=> ["_key"],
        "defaultOperator"=> "&&",
        "allowPragma" => false,
        "allowColumn" => true,
        "allowUpdate"=> false,
        "allowLeadingNot" => false,
      }
      assert_equal(expected_search_condition,
                   convert(select_request)["queries"]["EmptyTable_result"]["condition"])
    end

    def test_allow_leading_not_only
      select_request = {
        "table"          => "EmptyTable",
        "match_columns"  => "_key",
        "query"          => "QueryTest",
        "query_flags"    => "ALLOW_LEADING_NOT",
      }
      expected_search_condition = {
        "query"  => "QueryTest",
        "matchTo"=> ["_key"],
        "defaultOperator"=> "&&",
        "allowPragma" => false,
        "allowColumn" => false,
        "allowUpdate"=> false,
        "allowLeadingNot" => true,
      }
      assert_equal(expected_search_condition,
                   convert(select_request)["queries"]["EmptyTable_result"]["condition"])
    end
  end

  class OffsetTest < self
    def assert_offset(expected_offset, offset)
      select_request = {
        "table"          => "EmptyTable",
        "output_columns" => "_id",
      }
      select_request["offset"] = offset unless offset.nil?

      expected_search_request = {
        "queries" => {
          "EmptyTable_result" => {
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
              "offset" => expected_offset,
              "limit" => 10,
            },
          },
        },
      }
      assert_equal(expected_search_request, convert(select_request))
    end

    def test_zero
      assert_offset(0, "0")
    end

    def test_large
      assert_offset(100, "100")
    end

    def test_integer
      assert_offset(100, 100)
    end

    def test_default
      assert_offset(0, nil)
    end
  end

  class LimitTest < self
    def assert_limit(expected_limit, limit)
      select_request = {
        "table"          => "EmptyTable",
        "output_columns" => "_id",
      }
      select_request["limit"] = limit unless limit.nil?

      expected_search_request = {
        "queries" => {
          "EmptyTable_result" => {
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
              "offset" => 0,
              "limit" => expected_limit,
            },
          },
        },
      }
      assert_equal(expected_search_request, convert(select_request))
    end

    def test_zero
      assert_limit(0, "0")
    end

    def test_large
      assert_limit(100, "100")
    end

    def test_negative
      assert_limit(-1, "-1")
    end

    def test_integer
      assert_limit(100, 100)
    end

    def test_default
      assert_limit(10, nil)
    end
  end

  class SortTest < self
    data(
      :only_keys => {
        :input => {
          :keys => "-_id",
        },
        :expected => {
          :keys => ["-_id"],
          :offset => 0,
          :limit => 10,
        },
      },
      :offset => {
        :input => {
          :keys => "-_id",
          :offset => 5,
        },
        :expected => {
          :keys => ["-_id"],
          :offset => 5,
          :limit => 10,
        },
      },
      :limit => {
        :input => {
          :keys => "-_id",
          :limit => 5,
        },
        :expected => {
          :keys => ["-_id"],
          :offset => 0,
          :limit => 5,
        },
      },
    )
    def test_sort(data)
      expected = data[:expected]
      input = data[:input]
      select_request = {
        "table"          => "EmptyTable",
        "output_columns" => "_id",
      }
      select_request["sortby"] = input[:keys]  unless input[:keys].nil?
      select_request["offset"] = input[:offset]  unless input[:offset].nil?
      select_request["limit"] = input[:limit]  unless input[:limit].nil?

      expected_search_request = {
        "queries" => {
          "EmptyTable_result" => {
            "source"   => "EmptyTable",
            "sortBy" => {
              "keys" => expected[:keys],
              "offset" => expected[:offset],
              "limit" => expected[:limit],
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
              "offset" => 0,
              "limit" => expected[:limit],
            },
          },
        },
      }
      assert_equal(expected_search_request, convert(select_request))
    end
  end

  class DrilldownTest < self
    def base_request
      {
        "table"          => "EmptyTable",
        "output_columns" => "_id",
        "limit"          => 1,
      }
    end

    def base_queries
      {
        "EmptyTable_result" => {
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
            "offset" => 0,
            "limit" => 1,
          },
        },
      }
    end

    def test_simple
      select_request = base_request.merge({
        "drilldown"                => "a,b",
        "drilldown_output_columns" => "_key,_nsubrecs",
      })

      expected_output = {
        "elements"   => [
          "count",
          "attributes",
          "records",
        ],
        "attributes" => ["_key", "_nsubrecs"],
        "offset" => 0,
        "limit" => 10,
      }
      expected_search_request = {
        "queries" => base_queries.merge({
          "drilldown_result_a" => {
            "source" => "EmptyTable_result",
            "groupBy" => "a",
            "output" => expected_output,
          },
          "drilldown_result_b" => {
            "source" => "EmptyTable_result",
            "groupBy" => "b",
            "output" => expected_output,
          },
        }),
      }
      assert_equal(expected_search_request, convert(select_request))
    end

    def test_sorted
      select_request = base_request.merge({
        "drilldown"                => "a,b",
        "drilldown_output_columns" => "_key,_nsubrecs",
        "drilldown_sortby"         => "-_nsubrecs",
        "drilldown_offset"         => "1",
        "drilldown_limit"          => "2",
      })

      expected_sort_by = {
        "keys"   => ["-_nsubrecs"],
        "offset" => 1,
        "limit"  => 2,
      }
      expected_output = {
        "elements"   => [
          "count",
          "attributes",
          "records",
        ],
        "attributes" => ["_key", "_nsubrecs"],
        "limit" => 2,
      }
      expected_search_request = {
        "queries" => base_queries.merge({
          "drilldown_result_a" => {
            "source" => "EmptyTable_result",
            "groupBy" => "a",
            "sortBy" => expected_sort_by,
            "output" => expected_output,
          },
          "drilldown_result_b" => {
            "source" => "EmptyTable_result",
            "groupBy" => "b",
            "sortBy" => expected_sort_by,
            "output" => expected_output,
          },
        }),
      }
      assert_equal(expected_search_request, convert(select_request))
    end

    def test_only_pagination
      select_request = base_request.merge({
        "drilldown"                => "a,b",
        "drilldown_output_columns" => "_key,_nsubrecs",
        "drilldown_sortby"         => "-_nsubrecs",
        "drilldown_offset"         => "1",
        "drilldown_limit"          => "2",
      })

      expected_sort_by = {
        "keys"   => ["-_nsubrecs"],
        "offset" => 1,
        "limit"  => 2,
      }
      expected_output = {
        "elements"   => [
          "count",
          "attributes",
          "records",
        ],
        "attributes" => ["_key", "_nsubrecs"],
        "limit" => 2,
      }
      expected_search_request = {
        "queries" => base_queries.merge({
          "drilldown_result_a" => {
            "source" => "EmptyTable_result",
            "groupBy" => "a",
            "sortBy" => expected_sort_by,
            "output" => expected_output,
          },
          "drilldown_result_b" => {
            "source" => "EmptyTable_result",
            "groupBy" => "b",
            "sortBy" => expected_sort_by,
            "output" => expected_output,
          },
        }),
      }
      assert_equal(expected_search_request, convert(select_request))
    end
  end
end
