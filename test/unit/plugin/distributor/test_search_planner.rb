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

require "droonga/plugin/distributor/distributed_search_planner"

class DistributedSearchPlannerTest < Test::Unit::TestCase
  def plan(search_request)
    planner = Droonga::DistributedSearchPlanner.new(search_request)
    planner.build_messages
    planner.messages
  end

  def messages
    @messages ||= plan(@request)
  end

  def broadcast_message
    messages.find do |message|
      message["type"] == "broadcast"
    end
  end

  def reduce_message
    messages.find do |message|
      message["type"] == "search_reduce"
    end
  end

  def gather_message
    messages.find do |message|
      message["type"] == "search_gather"
    end
  end

  def dependencies
    messages.collect do |message|
      {
        "type"    => message["type"],
        "inputs"  => message["inputs"],
        "outputs" => message["outputs"],
      }
    end
  end

  class MultipleQueriesTest < self
    class MultipleOutputsTest < self
      def setup
        @request = {
          "type" => "search",
          "dataset" => "Droonga",
          "body" => {
            "queries" => {
              "query1" => {
                "source" => "User",
                "output" => {
                  "elements" => ["count"],
                },
              },
              "query2" => {
                "source" => "User",
                "output" => {
                  "elements" => ["count"],
                },
              },
              "query3" => {
                "source" => "User",
                "output" => {
                  "elements" => ["count"],
                },
              },
            },
          },
        }
      end

      def test_dependencies
        search_reduce_inputs = [
          "errors",
          "query1",
          "query2",
          "query3",
        ]
        search_gather_inputs = [
          "errors_reduced",
          "query1_reduced",
          "query2_reduced",
          "query3_reduced",
        ]
        assert_equal([
                       {
                         "type"    => "search_reduce",
                         "inputs"  => search_reduce_inputs,
                         "outputs" => search_gather_inputs,
                       },
                       {
                         "type"    => "search_gather",
                         "inputs"  => search_gather_inputs,
                         "outputs" => nil,
                       },
                       {
                         "type"    => "broadcast",
                         "inputs"  => nil,
                         "outputs" => search_reduce_inputs,
                       },
                     ],
                     dependencies)
      end
    end
  end

  class OutputTest < self
    class NothingTest < self
      def setup
        @request = {
          "type" => "search",
          "dataset" => "Droonga",
          "body" => {
            "queries" => {
              "no_output" => {
                "source" => "User",
              },
            },
          },
        }
      end

      def test_dependencies
        search_reduce_inputs = ["errors"]
        search_gather_inputs = ["errors_reduced"]
        assert_equal([
                       {
                         "type"    => "search_reduce",
                         "inputs"  => search_reduce_inputs,
                         "outputs" => search_gather_inputs,
                       },
                       {
                         "type"    => "search_gather",
                         "inputs"  => search_gather_inputs,
                         "outputs" => nil,
                       },
                       {
                         "type"    => "broadcast",
                         "inputs"  => nil,
                         "outputs" => search_reduce_inputs,
                       },
                     ],
                     dependencies)
      end

      def test_broadcast_body
        assert_equal({
                       "queries" => {
                         "no_output" => {
                           "source" => "User",
                         },
                       },
                     },
                     broadcast_message["body"])
      end

      def test_reduce_body
        assert_equal({
                       "errors" => {
                         "errors_reduced" => {
                           "type"  => "sum",
                           "limit" => -1,
                         },
                       },
                     },
                     reduce_message["body"])
      end

      def test_gather_body
        assert_equal({
                       "errors_reduced" => {
                         "output" => "errors",
                       },
                     },
                     gather_message["body"])
      end
    end

    class FormatTest < self
      def setup
        @output = {
          "format"     => "complex",
          "elements"   => ["records"],
          "attributes" => ["_id"],
          "offset"     => 0,
          "limit"      => 10,
        }
        @request = {
          "type" => "search",
          "dataset" => "Droonga",
          "body" => {
            "queries" => {
              "users" => {
                "source" => "User",
                "output" => @output,
              },
            },
          },
        }
      end

      def test_dependencies
        search_reduce_inputs = [
          "errors",
          "users",
        ]
        search_gather_inputs = [
          "errors_reduced",
          "users_reduced",
        ]
        assert_equal([
                       {
                         "type"    => "search_reduce",
                         "inputs"  => search_reduce_inputs,
                         "outputs" => search_gather_inputs,
                       },
                       {
                         "type"    => "search_gather",
                         "inputs"  => search_gather_inputs,
                         "outputs" => nil,
                       },
                       {
                         "type"    => "broadcast",
                         "inputs"  => nil,
                         "outputs" => search_reduce_inputs,
                       },
                     ],
                     dependencies)
      end

      def test_broadcast_body
        changed_output_parameters = {
          "format" => "simple"
        }
        assert_equal({
                       "queries" => {
                         "users" => {
                           "source" => "User",
                           "output" => @output.merge(changed_output_parameters),
                         },
                       },
                     },
                     broadcast_message["body"])
      end

      def test_gather_records
        records = @output.merge("type" => "sort")
        records.delete("elements")
        assert_equal({
                       "elements" => {
                         "records" => records,
                       },
                       "output" => "users",
                     },
                     gather_message["body"]["users_reduced"])
      end
    end
  end

  class SingleQueryTest < self
    def test_no_records_element
      request = {
        "type" => "search",
        "dataset" => "Droonga",
        "body" => {
          "queries" => {
            "no_records" => {
              "source" => "User",
              "sortBy" => {
                "keys" => ["name"],
                "offset" => 0,
                "limit" => 1,
              },
              "output" => {
                "elements" => ["count"],
              },
            },
          },
        },
      }

      expected_plan = []
      expected_plan << reducer(request, {
        "count" => {
          "type" => "sum",
        },
      })
      expected_plan << gatherer(request)
      expected_plan << searcher(request, :sort_limit => 1,
                                         :output_limit => 0)
      assert_equal(expected_plan, plan(request))
    end

    def test_no_output_limit
      request = {
        "type" => "search",
        "dataset" => "Droonga",
        "body" => {
          "queries" => {
            "no_limit" => {
              "source" => "User",
              "output" => {
                "format" => "complex",
                "elements" => ["count", "records"],
              },
            },
          },
        },
      }

      expected_plan = []
      expected_plan << reducer(request, {
        "count" => {
          "type" => "sum",
        },
      })
      expected_plan << gatherer(request)
      expected_plan << searcher(request, :output_offset => 0,
                                         :output_limit => 0)
      assert_equal(expected_plan, plan(request))
    end

    def test_have_records
      request = {
        "type" => "search",
        "dataset" => "Droonga",
        "body" => {
          "queries" => {
            "have_records" => {
              "source" => "User",
              "output" => {
                "format" => "complex",
                "elements" => ["records"],
                "attributes" => ["_key", "name", "age"],
                "offset" => 0,
                "limit" => 1,
              },
            },
          },
        },
      }

      expected_plan = []
      expected_plan << reducer(request, {
        "records" => {
          "type" => "sort",
          "operators" => [],
          "limit" => 1,
        },
      })
      expected_plan << gatherer(request, :elements => {
                                           "records" => records_mapper(
                                             :offset => 0,
                                             :limit => 1,
                                             :format => "complex",
                                             :attributes => ["_key", "name", "age"],
                                           ),
                                         })
      expected_plan << searcher(request, :output_offset => 0,
                                         :output_limit => 1)
      assert_equal(expected_plan, plan(request))
    end

    def test_have_output_offset
      request = {
        "type" => "search",
        "dataset" => "Droonga",
        "body" => {
          "queries" => {
            "have_records" => {
              "source" => "User",
              "output" => {
                "format" => "complex",
                "elements" => ["records"],
                "attributes" => ["_key", "name", "age"],
                "offset" => 1,
                "limit" => 1,
              },
            },
          },
        },
      }

      expected_plan = []
      expected_plan << reducer(request, {
        "records" => {
          "type" => "sort",
          "operators" => [],
          "limit" => 2,
        },
      })
      expected_plan << gatherer(request, :elements => {
                                           "records" => records_mapper(
                                             :offset => 1,
                                             :limit => 1,
                                             :format => "complex",
                                             :attributes => ["_key", "name", "age"],
                                           ),
                                         })
      expected_plan << searcher(request, :output_offset => 0,
                                         :output_limit => 2)
      assert_equal(expected_plan, plan(request))
    end

    def test_have_simple_sortBy
      request = {
        "type" => "search",
        "dataset" => "Droonga",
        "body" => {
          "queries" => {
            "have_records" => {
              "source" => "User",
              "sortBy" => ["name"],
              "output" => {
                "format" => "complex",
                "elements" => ["records"],
                "attributes" => ["_key", "name", "age"],
                "offset" => 0,
                "limit" => 1,
              },
            },
          },
        },
      }

      expected_plan = []
      expected_plan << reducer(request, {
        "records" => {
          "type" => "sort",
          "operators" => [
            { "column" => 1, "operator" => "<" },
          ],
          "limit" => 1,
        },
      })
      expected_plan << gatherer(request, :elements => {
                                           "records" => records_mapper(
                                             :offset => 0,
                                             :limit => 1,
                                             :format => "complex",
                                             :attributes => ["_key", "name", "age"],
                                           ),
                                         })
      expected_plan << searcher(request, :output_offset => 0,
                                         :output_limit => 1)
      assert_equal(expected_plan, plan(request))
    end

    def test_have_sortBy
      request = {
        "type" => "search",
        "dataset" => "Droonga",
        "body" => {
          "queries" => {
            "have_records" => {
              "source" => "User",
              "sortBy" => {
                "keys" => ["name"],
              },
              "output" => {
                "format" => "complex",
                "elements" => ["records"],
                "attributes" => ["_key", "name", "age"],
                "offset" => 0,
                "limit" => 1,
              },
            },
          },
        },
      }

      expected_plan = []
      expected_plan << reducer(request, {
        "records" => {
          "type" => "sort",
          "operators" => [
            { "column" => 1, "operator" => "<" },
          ],
          "limit" => 1,
        },
      })
      expected_plan << gatherer(request, :elements => {
                                           "records" => records_mapper(
                                             :offset => 0,
                                             :limit => 1,
                                             :format => "complex",
                                             :attributes => ["_key", "name", "age"],
                                           ),
                                         })
      expected_plan << searcher(request, :sort_offset => 0,
                                         :sort_limit => 1,
                                         :output_offset => 0,
                                         :output_limit => 1)
      assert_equal(expected_plan, plan(request))
    end

    def test_have_sortBy_offset_limit
      request = {
        "type" => "search",
        "dataset" => "Droonga",
        "body" => {
          "queries" => {
            "have_records" => {
              "source" => "User",
              "sortBy" => {
                "keys" => ["name"],
                "offset" => 1,
                "limit" => 2,
              },
              "output" => {
                "format" => "complex",
                "elements" => ["records"],
                "attributes" => ["_key", "name", "age"],
                "offset" => 4,
                "limit" => 8,
              },
            },
          },
        },
      }

      sort_limit = 1 + 4 + [2, 8].max
      output_limit = 1 + 4 + [2, 8].min
      expected_plan = []
      expected_plan << reducer(request, {
        "records" => {
          "type" => "sort",
          "operators" => [
            { "column" => 1, "operator" => "<" },
          ],
          "limit" => output_limit,
        },
      })
      expected_plan << gatherer(request, :elements => {
                                           "records" => records_mapper(
                                             :offset => 5,
                                             :limit => 2,
                                             :format => "complex",
                                             :attributes => ["_key", "name", "age"],
                                           ),
                                         })
      expected_plan << searcher(request, :sort_offset => 0,
                                         :sort_limit => sort_limit,
                                         :output_offset => 0,
                                         :output_limit => output_limit)
      assert_equal(expected_plan, plan(request))
    end

    def test_have_sortBy_with_infinity_output_limit
      request = {
        "type" => "search",
        "dataset" => "Droonga",
        "body" => {
          "queries" => {
            "have_records" => {
              "source" => "User",
              "sortBy" => {
                "keys" => ["name"],
                "offset" => 1,
                "limit" => 2,
              },
              "output" => {
                "format" => "complex",
                "elements" => ["records"],
                "attributes" => ["_key", "name", "age"],
                "offset" => 4,
                "limit" => -1,
              },
            },
          },
        },
      }

      limit = 1 + 4 + 2
      expected_plan = []
      expected_plan << reducer(request, {
        "records" => {
          "type" => "sort",
          "operators" => [
            { "column" => 1, "operator" => "<" },
          ],
          "limit" => limit,
        },
      })
      expected_plan << gatherer(request, :elements => {
                                           "records" => records_mapper(
                                             :offset => 5,
                                             :limit => 2,
                                             :format => "complex",
                                             :attributes => ["_key", "name", "age"],
                                           ),
                                         })
      expected_plan << searcher(request, :sort_offset => 0,
                                         :sort_limit => limit,
                                         :output_offset => 0,
                                         :output_limit => limit)
      assert_equal(expected_plan, plan(request))
    end

    def test_have_sortBy_with_infinity_sort_limit
      request = {
        "type" => "search",
        "dataset" => "Droonga",
        "body" => {
          "queries" => {
            "have_records" => {
              "source" => "User",
              "sortBy" => {
                "keys" => ["name"],
                "offset" => 1,
                "limit" => -1,
              },
              "output" => {
                "format" => "complex",
                "elements" => ["records"],
                "attributes" => ["_key", "name", "age"],
                "offset" => 4,
                "limit" => 8,
              },
            },
          },
        },
      }

      limit = 1 + 4 + 8
      expected_plan = []
      expected_plan << reducer(request, {
        "records" => {
          "type" => "sort",
          "operators" => [
            { "column" => 1, "operator" => "<" },
          ],
          "limit" => limit,
        },
      })
      expected_plan << gatherer(request, :elements => {
                                           "records" => records_mapper(
                                             :offset => 5,
                                             :limit => 8,
                                             :format => "complex",
                                             :attributes => ["_key", "name", "age"],
                                           ),
                                         })
      expected_plan << searcher(request, :sort_offset => 0,
                                         :sort_limit => limit,
                                         :output_offset => 0,
                                         :output_limit => limit)
      assert_equal(expected_plan, plan(request))
    end

    def test_have_sortBy_with_infinity_limit
      request = {
        "type" => "search",
        "dataset" => "Droonga",
        "body" => {
          "queries" => {
            "have_records" => {
              "source" => "User",
              "sortBy" => {
                "keys" => ["name"],
                "offset" => 1,
                "limit" => -1,
              },
              "output" => {
                "format" => "complex",
                "elements" => ["records"],
                "attributes" => ["_key", "name", "age"],
                "offset" => 4,
                "limit" => -1,
              },
            },
          },
        },
      }

      expected_plan = []
      expected_plan << reducer(request, {
        "records" => {
          "type" => "sort",
          "operators" => [
            { "column" => 1, "operator" => "<" },
          ],
          "limit" => -1,
        },
      })
      expected_plan << gatherer(request, :elements => {
                                           "records" => records_mapper(
                                             :offset => 5,
                                             :limit => -1,
                                             :format => "complex",
                                             :attributes => ["_key", "name", "age"],
                                           ),
                                         })
      expected_plan << searcher(request, :sort_offset => 0,
                                         :sort_limit => -1,
                                         :output_offset => 0,
                                         :output_limit => -1)
      assert_equal(expected_plan, plan(request))
    end

    def test_have_sortBy_with_multiple_sort_keys
      request = {
        "type" => "search",
        "dataset" => "Droonga",
        "body" => {
          "queries" => {
            "have_records" => {
              "source" => "User",
              "sortBy" => {
                "keys" => ["-age", "name"],
                "limit" => -1,
              },
              "output" => {
                "format" => "complex",
                "elements" => ["records"],
                "attributes" => ["_key", "name", "age"],
                "limit" => -1,
              },
            },
          },
        },
      }

      expected_plan = []
      expected_plan << reducer(request, {
        "records" => {
          "type" => "sort",
          "operators" => [
            { "column" => 2, "operator" => ">" },
            { "column" => 1, "operator" => "<" },
          ],
          "limit" => -1,
        },
      })
      expected_plan << gatherer(request, :elements => {
                                           "records" => records_mapper(
                                             :offset => 0,
                                             :limit => -1,
                                             :format => "complex",
                                             :attributes => ["_key", "name", "age"],
                                           ),
                                         })
      expected_plan << searcher(request, :sort_offset => 0,
                                         :sort_limit => -1,
                                         :output_offset => 0,
                                         :output_limit => -1)
      assert_equal(expected_plan, plan(request))
    end

    def test_have_sortBy_with_missing_sort_attributes
      request = {
        "type" => "search",
        "dataset" => "Droonga",
        "body" => {
          "queries" => {
            "have_records" => {
              "source" => "User",
              "sortBy" => {
                "keys" => ["-public_age", "public_name"],
                "limit" => -1,
              },
              "output" => {
                "format" => "complex",
                "elements" => ["records"],
                "attributes" => ["_key", "name", "age"],
                "limit" => -1,
              },
            },
          },
        },
      }

      expected_plan = []
      expected_plan << reducer(request, {
        "records" => {
          "type" => "sort",
          "operators" => [
            { "column" => 3, "operator" => ">" },
            { "column" => 4, "operator" => "<" },
          ],
          "limit" => -1,
        },
      })
      expected_plan << gatherer(request, :elements => {
                                           "records" => records_mapper(
                                             :offset => 0,
                                             :limit => -1,
                                             :format => "complex",
                                             :attributes => ["_key", "name", "age"],
                                           ),
                                         })
      expected_plan << searcher(request, :sort_offset => 0,
                                         :sort_limit => -1,
                                         :output_offset => 0,
                                         :output_limit => -1,
                                         :extra_attributes => ["public_age", "public_name"])
      assert_equal(expected_plan, plan(request))
    end

    def test_hash_attributes
      request = {
        "type" => "search",
        "dataset" => "Droonga",
        "body" => {
          "queries" => {
            "have_records" => {
              "source" => "User",
              "sortBy" => {
                "keys" => ["-public_age", "public_name"],
                "limit" => -1,
              },
              "output" => {
                "format" => "complex",
                "elements" => ["records"],
                "attributes" => {
                  "id" => "_key",
                  "name" => { "source" => "name" },
                  "age" => { "source" => "age" },
                },
                "limit" => -1,
              },
            },
          },
        },
      }

      expected_plan = []
      expected_plan << reducer(request, {
        "records" => {
          "type" => "sort",
          "operators" => [
            { "column" => 3, "operator" => ">" },
            { "column" => 4, "operator" => "<" },
          ],
          "limit" => -1,
        },
      })
      expected_plan << gatherer(request, :elements => {
                                           "records" => records_mapper(
                                             :offset => 0,
                                             :limit => -1,
                                             :format => "complex",
                                             :attributes => ["id", "name", "age"],
                                           ),
                                         })
      expected_plan << searcher(request, :sort_offset => 0,
                                         :sort_limit => -1,
                                         :output_offset => 0,
                                         :output_limit => -1,
                                         :extra_attributes => ["public_age", "public_name"])
      assert_equal(expected_plan, plan(request))
    end

    def test_groupBy
      request = {
        "type" => "search",
        "dataset" => "Droonga",
        "body" => {
          "queries" => {
            "grouped_records" => {
              "source" => "User",
              "groupBy" => "family_name",
              "output" => {
                "format" => "complex",
                "elements" => ["records"],
                "attributes" => ["_key", "_nsubrecs"],
                "limit" => -1,
              },
            },
          },
        },
      }

      expected_plan = []
      expected_plan << reducer(request, {
        "records" => {
          "type" => "sort",
          "operators" => [],
          "key_column" => 0,
          "limit" => -1,
        },
      })
      expected_plan << gatherer(request, :elements => {
                                           "records" => records_mapper(
                                             :offset => 0,
                                             :limit => -1,
                                             :format => "complex",
                                             :attributes => ["_key", "_nsubrecs"],
                                           ),
                                         })
      expected_plan << searcher(request, :output_offset => 0,
                                         :output_limit => -1,
                                         :unifiable => true)
      assert_equal(expected_plan, plan(request))
    end

    def test_groupBy_count
      request = {
        "type" => "search",
        "dataset" => "Droonga",
        "body" => {
          "queries" => {
            "grouped_records" => {
              "source" => "User",
              "groupBy" => "family_name",
              "output" => {
                "elements" => ["count"],
              },
            },
          },
        },
      }

      expected_plan = []
      expected_plan << reducer(request, {
        "count" => {
          "type" => "sum",
        },
        "records" => {
          "type" => "sort",
          "operators" => [],
          "key_column" => 0,
          "limit" => -1,
        },
      })
      expected_plan << gatherer(request, :elements => {
                                           "count" => count_mapper,
                                           "records" => records_mapper(
                                             :limit => -1,
                                             :attributes => ["_key"],
                                             :no_output => true,
                                           ),
                                         })
      expected_plan << searcher(request, :output_limit => -1,
                                         :extra_attributes => ["_key"],
                                         :extra_elements => ["records"],
                                         :unifiable => true)
      assert_equal(expected_plan, plan(request))
    end

    def test_groupBy_hash
      request = {
        "type" => "search",
        "dataset" => "Droonga",
        "body" => {
          "queries" => {
            "grouped_records" => {
              "source" => "User",
              "groupBy" => {
                "key" => "family_name",
                "maxNSubRecords" => 3,
              },
              "output" => {
                "format" => "complex",
                "elements" => ["records"],
                "attributes" => [
                  { "label" => "family_name", "source" => "_key" },
                  { "label" => "count", "source" => "_nsubrecs" },
                  { "label" => "users",
                    "source" => "_subrecs",
                    "attributes" => ["name", "age"] },
                ],
                "limit" => -1,
              },
            },
          },
        },
      }

      expected_plan = []
      expected_plan << reducer(request, {
        "records" => {
          "type" => "sort",
          "operators" => [],
          "key_column" => 3, # 0=family_name, 1=_nsubrecs, 2=_subrecs, 3=_keys
          "limit" => -1,
        },
      })
      expected_plan << gatherer(request, :elements => {
                                           "records" => records_mapper(
                                             :offset => 0,
                                             :limit => -1,
                                             :format => "complex",
                                             :attributes => ["family_name", "count", "users"],
                                           ),
                                         })
      expected_plan << searcher(request, :output_offset => 0,
                                         :output_limit => -1,
                                         :extra_attributes => ["_key"],
                                         :unifiable => true)
      assert_equal(expected_plan, plan(request))
    end

    private
    def reducer(search_request, reducer_body)
      queries = search_request["body"]["queries"]
      query_name = queries.keys.first

      reducer = {
        "type" => "search_reduce",
        "body" => {
          query_name => {
            "#{query_name}_reduced" => reducer_body,
          },
        },
        "inputs" => [query_name],
        "outputs" => ["#{query_name}_reduced"],
      }

      reducer
    end

    def count_mapper(options={})
      mapper = {
        "type" => "count",
        "target" => "records",
      }
      mapper
    end

    def records_mapper(options={})
      mapper = {
        "type" => "sort",
        "offset" => options[:offset] || 0,
        "limit" => options[:limit] || 0,
        "format" => options[:format] || "simple",
        "attributes" => options[:attributes] || [],
      }
      unless options[:no_output].nil?
        mapper["no_output"] = options[:no_output]
      end
      mapper
    end

    def gatherer(search_request, options={})
      queries = search_request["body"]["queries"]
      query_name = queries.keys.first

      gatherer = {
        "type" => "search_gather",
        "body" => {
        },
        "inputs" => [
        ],
        "post" => true,
      }

      unless options[:no_output]
        output = {
          "output" => query_name,
          "elements" => options[:elements] || {},
        }
        gatherer["body"]["#{query_name}_reduced"] = output
        gatherer["inputs"] << "#{query_name}_reduced"
      end

      gatherer
    end

    def searcher(search_request, options={})
      # dup and clone don't copy it deeply...
      searcher = Marshal.load(Marshal.dump(search_request))

      queries = searcher["body"]["queries"]
      query_name = queries.keys.first
      query = queries.values.first
      if options[:extra_attributes]
        attributes = query["output"]["attributes"] || []
        if attributes.is_a?(Hash)
          array_attributes = attributes.collect do |label, attribute|
            case attribute
            when Hash
              attribute["label"] = label
            when String
              attribute = { "label" => label, "source" => attribute }
            end
            attribute
          end
          attributes = array_attributes
        end
        query["output"]["attributes"] = attributes + options[:extra_attributes]
      end
      if options[:extra_elements]
        query["output"]["elements"] += options[:extra_elements]
      end
      if options[:sort_offset]
        query["sortBy"]["offset"] = options[:sort_offset]
      end
      if options[:sort_limit]
        query["sortBy"]["limit"] = options[:sort_limit]
      end
      if options[:output_offset]
        query["output"]["offset"] = options[:output_offset]
      end
      if options[:output_limit]
        query["output"]["limit"] = options[:output_limit]
      end

      query["output"]["format"] = "simple" if query["output"]
      query["output"]["unifiable"] = true if options[:unifiable]

      outputs = []
      outputs << query_name unless options[:no_output]

      searcher["type"] = "broadcast"
      searcher["command"] = "search"
      searcher["outputs"] = outputs
      searcher["replica"] = "random"
      searcher
    end
  end
end
