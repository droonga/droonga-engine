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
    dependencies = messages.collect do |message|
      {
        "type"    => message["type"],
        "inputs"  => message["inputs"],
        "outputs" => message["outputs"],
      }
    end
    sort_dependencies(dependencies)
  end

  def sort_dependencies(dependencies)
    dependencies.sort do |a, b|
      a["type"] <=> b["type"]
    end
  end

  def expected_dependencies(reduce_inputs, gather_inputs)
    dependencies = [
      {
        "type"    => "search_reduce",
        "inputs"  => reduce_inputs,
        "outputs" => gather_inputs,
      },
      {
        "type"    => "search_gather",
        "inputs"  => gather_inputs,
        "outputs" => nil,
      },
      {
        "type"    => "broadcast",
        "inputs"  => nil,
        "outputs" => reduce_inputs,
      },
    ]
    sort_dependencies(dependencies)
  end

  class MultipleQueriesTest < self
    class MultipleOutputsTest < self
      def setup
        @request = {
          "type"    => "search",
          "dataset" => "Droonga",
          "body"    => {
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
            },
          },
        }
      end

      def test_dependencies
        reduce_inputs = [
          "errors",
          "query1",
          "query2",
        ]
        gather_inputs = [
          "errors_reduced",
          "query1_reduced",
          "query2_reduced",
        ]
        assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                     dependencies)
      end
    end
  end

  # this should be moved to the test for DistributedCommandPlanner
  class BasicTest < self
    def setup
      @request = {
        "type"    => "search",
        "dataset" => "Droonga",
        "body"    => {
          "queries" => {
            "no_output" => {
              "source" => "User",
            },
          },
        },
      }
    end

    def test_dependencies
      reduce_inputs = ["errors"]
      gather_inputs = ["errors_reduced"]
      assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                   dependencies)
    end

    def test_broadcast_message_metadata
      message = broadcast_message
      metadata = {
        "command" => message["command"],
        "dataset" => message["dataset"],
        "replica" => message["replica"],
      }
      expected = {
        "command" => @request["type"],
        "dataset" => @request["dataset"],
        "replica" => "random",
      }
      assert_equal(expected, metadata)
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

  class OutputTest < self
    class NoOutputTest < self
      def setup
        @request = {
          "type"    => "search",
          "dataset" => "Droonga",
          "body"    => {
            "queries" => {
              "users" => {
                "source" => "User",
              },
            },
          },
        }
      end

      def test_dependencies
        reduce_inputs = ["errors"]
        gather_inputs = ["errors_reduced"]
        assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                     dependencies)
      end

      def test_broadcast_body
        assert_equal({
                       "queries" => {
                         "users" => {
                           "source" => "User",
                         },
                       },
                     },
                     broadcast_message["body"])
      end
    end

    class NoOutputLimitTest < self
      def setup
        @request = {
          "type"    => "search",
          "dataset" => "Droonga",
          "body"    => {
            "queries" => {
              "users" => {
                "source" => "User",
                "output" => {
                  "format"   => "complex",
                  "elements" => ["count", "records"],
                },
              },
            },
          },
        }
      end

      def test_dependencies
        reduce_inputs = ["errors", "users"]
        gather_inputs = ["errors_reduced", "users_reduced"]
        assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                     dependencies)
      end

      def test_broadcast_body
        assert_equal({
                       "queries" => {
                         "users" => {
                           "source" => "User",
                           "output" => {
                             "format"   => "simple",
                             "elements" => ["count", "records"],
                           },
                         },
                       },
                     },
                     broadcast_message["body"])
      end

      def test_reduce_body
        assert_equal({
                       "users_reduced" => {
                         "count" => {
                           "type" => "sum",
                         },
                       },
                     },
                     reduce_message["body"]["users"])
      end

      def test_gather_body
        assert_equal({
                       "output" => "users",
                     },
                     gather_message["body"]["users_reduced"])
      end
    end

    class ElementsTest < self
      class CountTest < self
        def setup
          @request = {
            "type"    => "search",
            "dataset" => "Droonga",
            "body"    => {
              "queries" => {
                "users" => {
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
          reduce_inputs = ["errors", "users"]
          gather_inputs = ["errors_reduced", "users_reduced"]
          assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                       dependencies)
        end

        def test_broadcast_body
          assert_equal({
                         "queries" => {
                           "users" => {
                             "output" => {
                               "elements" => ["count"],
                             },
                             "source" => "User",
                           },
                         },
                       },
                       broadcast_message["body"])
        end

        def test_reduce_body
          assert_equal({
                         "users_reduced" => {
                           "count" => {
                             "type" => "sum",
                           },
                         },
                       },
                       reduce_message["body"]["users"])
        end

        def test_gather_body
          assert_equal({
                         "output" => "users",
                       },
                       gather_message["body"]["users_reduced"])
        end
      end

      class RecordsTest < self
        def setup
          @request = {
            "type"    => "search",
            "dataset" => "Droonga",
            "body"    => {
              "queries" => {
                "users" => {
                  "source" => "User",
                  "output" => {
                    "elements"   => ["records"],
                    "attributes" => ["_key"],
                    "limit"      => 1,
                  },
                },
              },
            },
          }
        end

        def test_dependencies
          reduce_inputs = ["errors", "users"]
          gather_inputs = ["errors_reduced", "users_reduced"]
          assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                       dependencies)
        end

        def test_broadcast_body
          assert_equal({
                         "queries" => {
                           "users" => {
                             "output" => {
                               "elements"   => ["records"],
                               "attributes" => ["_key"],
                               "limit"      => 1,
                             },
                             "source" => "User",
                           },
                         },
                       },
                       broadcast_message["body"])
        end

        def test_reduce_body
          assert_equal({
                         "users_reduced" => {
                           "records" => {
                             "type"      => "sort",
                             "operators" => [],
                             "limit"     => 1,
                           },
                         },
                       },
                       reduce_message["body"]["users"])
        end

        def test_gather_body
          assert_equal({
                       "elements" => {
                         "records" => {
                           "attributes" => ["_key"],
                           "limit"      => 1,
                         },
                       },
                         "output" => "users",
                       },
                       gather_message["body"]["users_reduced"])
        end
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
          "type"    => "search",
          "dataset" => "Droonga",
          "body"    => {
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
        reduce_inputs = ["errors", "users"]
        gather_inputs = ["errors_reduced", "users_reduced"]
        assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                     dependencies)
      end

      def test_broadcast_body
        changed_output_parameters = {
          "format" => "simple",
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

      def test_reduce_body
        assert_equal({
                       "users_reduced" => {
                         "records" => {
                           "type"      => "sort",
                           "operators" => [],
                           "limit"     => 10,
                         },
                       },
                     },
                     reduce_message["body"]["users"])
      end

      def test_gather_records
        assert_equal({
                       "elements" => {
                         "records" => {
                           "format"     => "complex",
                           "attributes" => ["_id"],
                           "limit"      => 10,
                         },
                       },
                       "output" => "users",
                     },
                     gather_message["body"]["users_reduced"])
      end
    end

    class OutputOffsetTest < self
      def setup
        @output = {
          "elements"   => ["records"],
          "attributes" => ["_key"],
          "offset"     => 1,
          "limit"      => 1,
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
        reduce_inputs = ["errors", "users"]
        gather_inputs = ["errors_reduced", "users_reduced"]
        assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                     dependencies)
      end

      def test_broadcast_body
        changed_output_parameters = {
          "offset" => 0,
          "limit"  => 2,
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

      def test_reduce_body
        assert_equal({
                       "users_reduced" => {
                         "records" => {
                           "type"      => "sort",
                           "operators" => [],
                           "limit"     => 2,
                         },
                       },
                     },
                     reduce_message["body"]["users"])
      end

      def test_gather_records
        assert_equal({
                       "elements" => {
                         "records" => {
                           "attributes" => ["_key"],
                           "offset"     => 1,
                           "limit"      => 1,
                         },
                       },
                       "output" => "users",
                     },
                     gather_message["body"]["users_reduced"])
      end
    end
  end

  class SortByTest < self
    class SimpleTest < self
      def setup
        @output = {
          "elements"   => ["records"],
          "attributes" => ["_key"],
          "limit"      => 1,
        }
        @request = {
          "type" => "search",
          "dataset" => "Droonga",
          "body" => {
            "queries" => {
              "users" => {
                "source" => "User",
                "sortBy" => ["_key"],
                "output" => @output,
              },
            },
          },
        }
      end

      def test_dependencies
        reduce_inputs = ["errors", "users"]
        gather_inputs = ["errors_reduced", "users_reduced"]
        assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                     dependencies)
      end

      def test_broadcast_body
        assert_equal({
                       "queries" => {
                         "users" => {
                           "source" => "User",
                           "sortBy" => ["_key"],
                           "output" => @output,
                         },
                       },
                     },
                     broadcast_message["body"])
      end

      def test_reduce_body
        assert_equal({
                       "users_reduced" => {
                         "records" => {
                           "type"      => "sort",
                           "operators" => [
                             { "column" => 0, "operator" => "<" },
                           ],
                           "limit"     => 1,
                         },
                       },
                     },
                     reduce_message["body"]["users"])
      end

      def test_gather_records
        assert_equal({
                       "elements" => {
                         "records" => {
                           "attributes" => ["_key"],
                           "limit"      => 1,
                         },
                       },
                       "output" => "users",
                     },
                     gather_message["body"]["users_reduced"])
      end
    end

    class SimpleHiddenColumnTest < self
      def setup
        @output = {
          "elements"   => ["records"],
          "attributes" => ["_key"],
          "limit"      => 1,
        }
        @request = {
          "type" => "search",
          "dataset" => "Droonga",
          "body" => {
            "queries" => {
              "users" => {
                "source" => "User",
                "sortBy" => ["name"],
                "output" => @output,
              },
            },
          },
        }
      end

      def test_dependencies
        reduce_inputs = ["errors", "users"]
        gather_inputs = ["errors_reduced", "users_reduced"]
        assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                     dependencies)
      end

      def test_broadcast_body
        changed_output_parameters = {
          "attributes" => ["_key", "name"],
        }
        assert_equal({
                       "queries" => {
                         "users" => {
                           "source" => "User",
                           "sortBy" => ["name"],
                           "output" => @output.merge(changed_output_parameters),
                         },
                       },
                     },
                     broadcast_message["body"])
      end

      def test_reduce_body
        assert_equal({
                       "users_reduced" => {
                         "records" => {
                           "type"      => "sort",
                           "operators" => [
                             { "column" => 1, "operator" => "<" },
                           ],
                           "limit"     => 1,
                         },
                       },
                     },
                     reduce_message["body"]["users"])
      end

      def test_gather_records
        assert_equal({
                       "elements" => {
                         "records" => {
                           "attributes" => ["_key"],
                           "limit"      => 1,
                         },
                       },
                       "output" => "users",
                     },
                     gather_message["body"]["users_reduced"])
      end
    end

    class ComplexTest < self
      def setup
        @output = {
          "elements"   => ["records"],
          "attributes" => ["_key"],
          "limit"      => 1,
        }
        @sort_by = {
          "keys" => ["_key"],
        }
        @request = {
          "type" => "search",
          "dataset" => "Droonga",
          "body" => {
            "queries" => {
              "users" => {
                "source" => "User",
                "sortBy" => @sort_by,
                "output" => @output,
              },
            },
          },
        }
      end

      def test_dependencies
        reduce_inputs = ["errors", "users"]
        gather_inputs = ["errors_reduced", "users_reduced"]
        assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                     dependencies)
      end

      def test_broadcast_body
        changed_sort_by_parameters = {
          "offset" => 0,
          "limit"  => 1,
        }
        assert_equal({
                       "queries" => {
                         "users" => {
                           "source" => "User",
                           "sortBy" => @sort_by.merge(changed_sort_by_parameters),
                           "output" => @output,
                         },
                       },
                     },
                     broadcast_message["body"])
      end

      def test_reduce_body
        assert_equal({
                       "users_reduced" => {
                         "records" => {
                           "type"      => "sort",
                           "operators" => [
                             { "column" => 0, "operator" => "<" },
                           ],
                           "limit"     => 1,
                         },
                       },
                     },
                     reduce_message["body"]["users"])
      end

      def test_gather_records
        assert_equal({
                       "elements" => {
                         "records" => {
                           "attributes" => ["_key"],
                           "limit"      => 1,
                         },
                       },
                       "output" => "users",
                     },
                     gather_message["body"]["users_reduced"])
      end
    end

    class ComplexHiddenColumnTest < self
      def setup
        @output = {
          "elements"   => ["records"],
          "attributes" => ["_key"],
          "limit"      => 1,
        }
        @sort_by = {
          "keys" => ["name"],
        }
        @request = {
          "type" => "search",
          "dataset" => "Droonga",
          "body" => {
            "queries" => {
              "users" => {
                "source" => "User",
                "sortBy" => @sort_by,
                "output" => @output,
              },
            },
          },
        }
      end

      def test_dependencies
        reduce_inputs = ["errors", "users"]
        gather_inputs = ["errors_reduced", "users_reduced"]
        assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                     dependencies)
      end

      def test_broadcast_body
        changed_sort_by_parameters = {
          "offset" => 0,
          "limit"  => 1,
        }
        changed_output_parameters = {
          "attributes" => ["_key", "name"],
        }
        assert_equal({
                       "queries" => {
                         "users" => {
                           "source" => "User",
                           "sortBy" => @sort_by.merge(changed_sort_by_parameters),
                           "output" => @output.merge(changed_output_parameters),
                         },
                       },
                     },
                     broadcast_message["body"])
      end

      def test_reduce_body
        assert_equal({
                       "users_reduced" => {
                         "records" => {
                           "type"      => "sort",
                           "operators" => [
                             { "column" => 1, "operator" => "<" },
                           ],
                           "limit"     => 1,
                         },
                       },
                     },
                     reduce_message["body"]["users"])
      end

      def test_gather_records
        assert_equal({
                       "elements" => {
                         "records" => {
                           "attributes" => ["_key"],
                           "limit"      => 1,
                         },
                       },
                       "output" => "users",
                     },
                     gather_message["body"]["users_reduced"])
      end
    end

    class WithHashAttributesTest < self
      def setup
        @output = {
          "elements"   => ["records"],
          "attributes" => {
            "id"   => "_key",
            "name" => { "source" => "name" },
          },
          "limit"      => 1,
        }
        @sort_by = {
          "keys" => ["hidden"],
        }
        @request = {
          "type" => "search",
          "dataset" => "Droonga",
          "body" => {
            "queries" => {
              "users" => {
                "source" => "User",
                "sortBy" => @sort_by,
                "output" => @output,
              },
            },
          },
        }
      end

      def test_dependencies
        reduce_inputs = ["errors", "users"]
        gather_inputs = ["errors_reduced", "users_reduced"]
        assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                     dependencies)
      end

      def test_broadcast_body
        changed_sort_by_parameters = {
          "offset" => 0,
          "limit"  => 1,
        }
        changed_output_parameters = {
          "attributes" => [
            { "label" => "id",   "source" => "_key" },
            { "label" => "name", "source" => "name" },
            "hidden",
          ],
        }
        assert_equal({
                       "queries" => {
                         "users" => {
                           "source" => "User",
                           "sortBy" => @sort_by.merge(changed_sort_by_parameters),
                           "output" => @output.merge(changed_output_parameters),
                         },
                       },
                     },
                     broadcast_message["body"])
      end

      def test_reduce_body
        assert_equal({
                       "users_reduced" => {
                         "records" => {
                           "type"      => "sort",
                           "operators" => [
                             { "column" => 2, "operator" => "<" },
                           ],
                           "limit"     => 1,
                         },
                       },
                     },
                     reduce_message["body"]["users"])
      end

      def test_gather_records
        assert_equal({
                       "elements" => {
                         "records" => {
                           "attributes" => ["id", "name"],
                           "limit"      => 1,
                         },
                       },
                       "output" => "users",
                     },
                     gather_message["body"]["users_reduced"])
      end
    end

    class WithComplexAttributesArrayTest < self
      def setup
        @output = {
          "elements"   => ["records"],
          "attributes" => [
            { "label" => "id",        "source" => "_key" },
            { "label" => "real_name", "source" => "name" },
          ],
          "limit"      => 1,
        }
        @sort_by = {
          "keys" => ["name"],
        }
        @request = {
          "type" => "search",
          "dataset" => "Droonga",
          "body" => {
            "queries" => {
              "users" => {
                "source" => "User",
                "sortBy" => @sort_by,
                "output" => @output,
              },
            },
          },
        }
      end

      def test_dependencies
        reduce_inputs = ["errors", "users"]
        gather_inputs = ["errors_reduced", "users_reduced"]
        assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                     dependencies)
      end

      def test_broadcast_body
        changed_sort_by_parameters = {
          "offset" => 0,
          "limit"  => 1,
        }
        changed_output_parameters = {
          "attributes" => [
            { "label" => "id",        "source" => "_key" },
            { "label" => "real_name", "source" => "name" },
          ],
        }
        assert_equal({
                       "queries" => {
                         "users" => {
                           "source" => "User",
                           "sortBy" => @sort_by.merge(changed_sort_by_parameters),
                           "output" => @output.merge(changed_output_parameters),
                         },
                       },
                     },
                     broadcast_message["body"])
      end

      def test_reduce_body
        assert_equal({
                       "users_reduced" => {
                         "records" => {
                           "type"      => "sort",
                           "operators" => [
                             { "column" => 1, "operator" => "<" },
                           ],
                           "limit"     => 1,
                         },
                       },
                     },
                     reduce_message["body"]["users"])
      end

      def test_gather_records
        assert_equal({
                       "elements" => {
                         "records" => {
                           "attributes" => ["id", "real_name"],
                           "limit"      => 1,
                         },
                       },
                       "output" => "users",
                     },
                     gather_message["body"]["users_reduced"])
      end
    end

    class MultipleColumnsTest < self
      def setup
        @output = {
          "elements"   => ["records"],
          "attributes" => ["_key"],
          "limit"      => 1,
        }
        @sort_by = {
          "keys" => ["-age", "name", "_key"],
        }
        @request = {
          "type" => "search",
          "dataset" => "Droonga",
          "body" => {
            "queries" => {
              "users" => {
                "source" => "User",
                "sortBy" => @sort_by,
                "output" => @output,
              },
            },
          },
        }
      end

      def test_dependencies
        reduce_inputs = ["errors", "users"]
        gather_inputs = ["errors_reduced", "users_reduced"]
        assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                     dependencies)
      end

      def test_broadcast_body
        changed_sort_by_parameters = {
          "offset" => 0,
          "limit"  => 1,
        }
        changed_output_parameters = {
          "attributes" => ["_key", "age", "name"],
        }
        assert_equal({
                       "queries" => {
                         "users" => {
                           "source" => "User",
                           "sortBy" => @sort_by.merge(changed_sort_by_parameters),
                           "output" => @output.merge(changed_output_parameters),
                         },
                       },
                     },
                     broadcast_message["body"])
      end

      def test_reduce_body
        assert_equal({
                       "users_reduced" => {
                         "records" => {
                           "type"      => "sort",
                           "operators" => [
                             { "column" => 1, "operator" => ">" },
                             { "column" => 2, "operator" => "<" },
                             { "column" => 0, "operator" => "<" },
                           ],
                           "limit"     => 1,
                         },
                       },
                     },
                     reduce_message["body"]["users"])
      end

      def test_gather_records
        assert_equal({
                       "elements" => {
                         "records" => {
                           "attributes" => ["_key"],
                           "limit"      => 1,
                         },
                       },
                       "output" => "users",
                     },
                     gather_message["body"]["users_reduced"])
      end
    end

    class OffsetLimitTest < self
      def max_limit
        [@sort_by["limit"], @output["limit"]].max
      end

      def min_limit
        [@sort_by["limit"], @output["limit"]].min
      end

      def total_offset
        @sort_by["offset"] + @output["offset"]
      end

      class RegularRangeTest < self
        def setup
          @output = {
            "elements"   => ["records"],
            "attributes" => ["_key"],
            "offset"     => 4,
            "limit"      => 8,
          }
          @sort_by = {
            "keys"   => ["_key"],
            "offset" => 1,
            "limit"  => 2,
          }
          @request = {
            "type" => "search",
            "dataset" => "Droonga",
            "body" => {
              "queries" => {
                "users" => {
                  "source" => "User",
                  "sortBy" => @sort_by,
                  "output" => @output,
                },
              },
            },
          }
        end

        def test_dependencies
          reduce_inputs = ["errors", "users"]
          gather_inputs = ["errors_reduced", "users_reduced"]
          assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                       dependencies)
        end

        def test_broadcast_body
          changed_sort_by_parameters = {
            "offset" => 0,
            "limit"  => total_offset + max_limit,
          }
          changed_output_parameters = {
            "offset" => 0,
            "limit"  => total_offset + min_limit,
          }
          assert_equal({
                         "queries" => {
                           "users" => {
                             "source" => "User",
                             "sortBy" => @sort_by.merge(changed_sort_by_parameters),
                             "output" => @output.merge(changed_output_parameters),
                           },
                         },
                       },
                       broadcast_message["body"])
        end

        def test_reduce_body
          assert_equal({
                         "users_reduced" => {
                           "records" => {
                             "type"      => "sort",
                             "operators" => [
                               { "column" => 0, "operator" => "<" },
                             ],
                             "limit"     => total_offset + min_limit,
                           },
                         },
                       },
                       reduce_message["body"]["users"])
        end

        def test_gather_records
          assert_equal({
                         "elements" => {
                           "records" => {
                             "attributes" => ["_key"],
                             "offset"     => total_offset,
                             "limit"      => min_limit,
                           },
                         },
                         "output" => "users",
                       },
                       gather_message["body"]["users_reduced"])
        end
      end

      class InfinitOutputLimitTest < self
        def setup
          @output = {
            "elements"   => ["records"],
            "attributes" => ["_key"],
            "offset"     => 4,
            "limit"      => -1,
          }
          @sort_by = {
            "keys"   => ["_key"],
            "offset" => 1,
            "limit"  => 2,
          }
          @request = {
            "type" => "search",
            "dataset" => "Droonga",
            "body" => {
              "queries" => {
                "users" => {
                  "source" => "User",
                  "sortBy" => @sort_by,
                  "output" => @output,
                },
              },
            },
          }
        end

        def test_dependencies
          reduce_inputs = ["errors", "users"]
          gather_inputs = ["errors_reduced", "users_reduced"]
          assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                       dependencies)
        end

        def test_broadcast_body
          changed_sort_by_parameters = {
            "offset" => 0,
            "limit"  => total_offset + max_limit,
          }
          changed_output_parameters = {
            "offset" => 0,
            "limit"  => total_offset + max_limit,
          }
          assert_equal({
                         "queries" => {
                           "users" => {
                             "source" => "User",
                             "sortBy" => @sort_by.merge(changed_sort_by_parameters),
                             "output" => @output.merge(changed_output_parameters),
                           },
                         },
                       },
                       broadcast_message["body"])
        end

        def test_reduce_body
          assert_equal({
                         "users_reduced" => {
                           "records" => {
                             "type"      => "sort",
                             "operators" => [
                               { "column" => 0, "operator" => "<" },
                             ],
                             "limit"     => total_offset + max_limit,
                           },
                         },
                       },
                       reduce_message["body"]["users"])
        end

        def test_gather_records
          assert_equal({
                         "elements" => {
                           "records" => {
                             "attributes" => ["_key"],
                             "offset"     => total_offset,
                             "limit"      => max_limit,
                           },
                         },
                         "output" => "users",
                       },
                       gather_message["body"]["users_reduced"])
        end
      end

      class InifinitSortLimitTest < self
        def setup
          @output = {
            "elements"   => ["records"],
            "attributes" => ["_key"],
            "offset"     => 4,
            "limit"      => 8,
          }
          @sort_by = {
            "keys"   => ["_key"],
            "offset" => 1,
            "limit"  => -1,
          }
          @request = {
            "type" => "search",
            "dataset" => "Droonga",
            "body" => {
              "queries" => {
                "users" => {
                  "source" => "User",
                  "sortBy" => @sort_by,
                  "output" => @output,
                },
              },
            },
          }
        end

        def test_dependencies
          reduce_inputs = ["errors", "users"]
          gather_inputs = ["errors_reduced", "users_reduced"]
          assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                       dependencies)
        end

        def test_broadcast_body
          changed_sort_by_parameters = {
            "offset" => 0,
            "limit"  => total_offset + max_limit,
          }
          changed_output_parameters = {
            "offset" => 0,
            "limit"  => total_offset + max_limit,
          }
          assert_equal({
                         "queries" => {
                           "users" => {
                             "source" => "User",
                             "sortBy" => @sort_by.merge(changed_sort_by_parameters),
                             "output" => @output.merge(changed_output_parameters),
                           },
                         },
                       },
                       broadcast_message["body"])
        end

        def test_reduce_body
          assert_equal({
                         "users_reduced" => {
                           "records" => {
                             "type"      => "sort",
                             "operators" => [
                               { "column" => 0, "operator" => "<" },
                             ],
                             "limit"     => total_offset + max_limit,
                           },
                         },
                       },
                       reduce_message["body"]["users"])
        end

        def test_gather_records
          assert_equal({
                         "elements" => {
                           "records" => {
                             "attributes" => ["_key"],
                             "offset"     => total_offset,
                             "limit"      => max_limit,
                           },
                         },
                         "output" => "users",
                       },
                       gather_message["body"]["users_reduced"])
        end
      end

      class InifinitBothLimitTest < self
        def setup
          @output = {
            "elements"   => ["records"],
            "attributes" => ["_key"],
            "offset"     => 4,
            "limit"      => -1,
          }
          @sort_by = {
            "keys"   => ["_key"],
            "offset" => 1,
            "limit"  => -1,
          }
          @request = {
            "type" => "search",
            "dataset" => "Droonga",
            "body" => {
              "queries" => {
                "users" => {
                  "source" => "User",
                  "sortBy" => @sort_by,
                  "output" => @output,
                },
              },
            },
          }
        end

        def test_dependencies
          reduce_inputs = ["errors", "users"]
          gather_inputs = ["errors_reduced", "users_reduced"]
          assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                       dependencies)
        end

        def test_broadcast_body
          changed_sort_by_parameters = {
            "offset" => 0,
            "limit"  => min_limit,
          }
          changed_output_parameters = {
            "offset" => 0,
            "limit"  => min_limit,
          }
          assert_equal({
                         "queries" => {
                           "users" => {
                             "source" => "User",
                             "sortBy" => @sort_by.merge(changed_sort_by_parameters),
                             "output" => @output.merge(changed_output_parameters),
                           },
                         },
                       },
                       broadcast_message["body"])
        end

        def test_reduce_body
          assert_equal({
                         "users_reduced" => {
                           "records" => {
                             "type"      => "sort",
                             "operators" => [
                               { "column" => 0, "operator" => "<" },
                             ],
                             "limit"     => min_limit,
                           },
                         },
                       },
                       reduce_message["body"]["users"])
        end

        def test_gather_records
          assert_equal({
                         "elements" => {
                           "records" => {
                             "attributes" => ["_key"],
                             "offset"     => total_offset,
                             "limit"      => min_limit,
                           },
                         },
                         "output" => "users",
                       },
                       gather_message["body"]["users_reduced"])
        end
      end
    end
  end

  class GroupByTest < self
    class SimpleTest < self
      def setup
        @output = {
          "elements"   => ["records"],
          "attributes" => ["_nsubrecs", "_key"],
          "limit"      => 1,
        }
        @group_by = "family_name"
        @request = {
          "type"    => "search",
          "dataset" => "Droonga",
          "body"    => {
            "queries" => {
              "families" => {
                "source"  => "User",
                "groupBy" => @group_by,
                "output"  => @output,
              },
            },
          },
        }
      end

      def test_dependencies
        reduce_inputs = ["errors", "families"]
        gather_inputs = ["errors_reduced", "families_reduced"]
        assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                     dependencies)
      end

      def test_broadcast_body
        changed_output_parameters = {
          "unifiable" => true,
        }
        assert_equal({
                       "queries" => {
                         "families" => {
                           "source"  => "User",
                           "groupBy" => @group_by,
                           "output"  => @output.merge(changed_output_parameters),
                         },
                       },
                     },
                     broadcast_message["body"])
      end

      def test_reduce_body
        assert_equal({
                       "families_reduced" => {
                         "records" => {
                           "type"      => "sort",
                           "operators" => [],
                          "key_column" => 1,
                           "limit"     => 1,
                         },
                       },
                     },
                     reduce_message["body"]["families"])
      end

      def test_gather_records
        assert_equal({
                       "elements" => {
                         "records" => {
                           "attributes" => ["_nsubrecs", "_key"],
                           "limit"      => 1,
                         },
                       },
                       "output" => "families",
                     },
                     gather_message["body"]["families_reduced"])
      end
    end

    class WithHashAttributesTest < self
      def setup
        @output = {
          "elements"   => ["records"],
          "attributes" => {
            "family_name" => "_key",
            "count"       => { "source" => "_nsubrecs" },
          },
          "limit"      => 1,
        }
        @group_by = "family_name"
        @request = {
          "type"    => "search",
          "dataset" => "Droonga",
          "body"    => {
            "queries" => {
              "families" => {
                "source"  => "User",
                "groupBy" => @group_by,
                "output"  => @output,
              },
            },
          },
        }
      end

      def test_dependencies
        reduce_inputs = ["errors", "families"]
        gather_inputs = ["errors_reduced", "families_reduced"]
        assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                     dependencies)
      end

      def test_broadcast_body
        changed_output_parameters = {
          "attributes" => [
            { "label" => "family_name", "source" => "_key" },
            { "label" => "count",       "source" => "_nsubrecs" },
          ],
          "unifiable" => true,
        }
        assert_equal({
                       "queries" => {
                         "families" => {
                           "source"  => "User",
                           "groupBy" => @group_by,
                           "output"  => @output.merge(changed_output_parameters),
                         },
                       },
                     },
                     broadcast_message["body"])
      end

      def test_reduce_body
        assert_equal({
                       "families_reduced" => {
                         "records" => {
                           "type"       => "sort",
                           "operators"  => [],
                           "key_column" => 0,
                           "limit"      => 1,
                         },
                       },
                     },
                     reduce_message["body"]["families"])
      end

      def test_gather_records
        assert_equal({
                       "elements" => {
                         "records" => {
                           "attributes" => ["family_name", "count"],
                           "limit"      => 1,
                         },
                       },
                       "output" => "families",
                     },
                     gather_message["body"]["families_reduced"])
      end
    end

    class WithHashAttributesMissingKeyTest < self
      def setup
        @output = {
          "elements"   => ["records"],
          "attributes" => {
            "count"       => { "source" => "_nsubrecs" },
          },
          "limit"      => 1,
        }
        @group_by = "family_name"
        @request = {
          "type"    => "search",
          "dataset" => "Droonga",
          "body"    => {
            "queries" => {
              "families" => {
                "source"  => "User",
                "groupBy" => @group_by,
                "output"  => @output,
              },
            },
          },
        }
      end

      def test_dependencies
        reduce_inputs = ["errors", "families"]
        gather_inputs = ["errors_reduced", "families_reduced"]
        assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                     dependencies)
      end

      def test_broadcast_body
        changed_output_parameters = {
          "attributes" => [
            { "label" => "count", "source" => "_nsubrecs" },
            "_key",
          ],
          "unifiable" => true,
        }
        assert_equal({
                       "queries" => {
                         "families" => {
                           "source"  => "User",
                           "groupBy" => @group_by,
                           "output"  => @output.merge(changed_output_parameters),
                         },
                       },
                     },
                     broadcast_message["body"])
      end

      def test_reduce_body
        assert_equal({
                       "families_reduced" => {
                         "records" => {
                           "type"       => "sort",
                           "operators"  => [],
                           "key_column" => 1,
                           "limit"      => 1,
                         },
                       },
                     },
                     reduce_message["body"]["families"])
      end

      def test_gather_records
        assert_equal({
                       "elements" => {
                         "records" => {
                           "attributes" => ["count"],
                           "limit"      => 1,
                         },
                       },
                       "output" => "families",
                     },
                     gather_message["body"]["families_reduced"])
      end
    end

    class WithComplexAttributesArrayTest < self
      def setup
        @output = {
          "elements"   => ["records"],
          "attributes" => [
            { "label" => "family_name", "source" => "_key" },
            { "label" => "count",       "source" => "_nsubrecs" },
          ],
          "limit"      => 1,
        }
        @group_by = "family_name"
        @request = {
          "type"    => "search",
          "dataset" => "Droonga",
          "body"    => {
            "queries" => {
              "families" => {
                "source"  => "User",
                "groupBy" => @group_by,
                "output"  => @output,
              },
            },
          },
        }
      end

      def test_dependencies
        reduce_inputs = ["errors", "families"]
        gather_inputs = ["errors_reduced", "families_reduced"]
        assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                     dependencies)
      end

      def test_broadcast_body
        changed_output_parameters = {
          "unifiable" => true,
        }
        assert_equal({
                       "queries" => {
                         "families" => {
                           "source"  => "User",
                           "groupBy" => @group_by,
                           "output"  => @output.merge(changed_output_parameters),
                         },
                       },
                     },
                     broadcast_message["body"])
      end

      def test_reduce_body
        assert_equal({
                       "families_reduced" => {
                         "records" => {
                           "type"       => "sort",
                           "operators"  => [],
                           "key_column" => 0,
                           "limit"      => 1,
                         },
                       },
                     },
                     reduce_message["body"]["families"])
      end

      def test_gather_records
        assert_equal({
                       "elements" => {
                         "records" => {
                           "attributes" => ["family_name", "count"],
                           "limit"      => 1,
                         },
                       },
                       "output" => "families",
                     },
                     gather_message["body"]["families_reduced"])
      end
    end

    class WithComplexAttributesArrayMissingKeyTest < self
      def setup
        @output = {
          "elements"   => ["records"],
          "attributes" => [
            { "label" => "count", "source" => "_nsubrecs" },
          ],
          "limit"      => 1,
        }
        @group_by = "family_name"
        @request = {
          "type"    => "search",
          "dataset" => "Droonga",
          "body"    => {
            "queries" => {
              "families" => {
                "source"  => "User",
                "groupBy" => @group_by,
                "output"  => @output,
              },
            },
          },
        }
      end

      def test_dependencies
        reduce_inputs = ["errors", "families"]
        gather_inputs = ["errors_reduced", "families_reduced"]
        assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                     dependencies)
      end

      def test_broadcast_body
        changed_output_parameters = {
          "attributes" => [
            { "label" => "count", "source" => "_nsubrecs" },
            "_key",
          ],
          "unifiable"  => true,
        }
        assert_equal({
                       "queries" => {
                         "families" => {
                           "source"  => "User",
                           "groupBy" => @group_by,
                           "output"  => @output.merge(changed_output_parameters),
                         },
                       },
                     },
                     broadcast_message["body"])
      end

      def test_reduce_body
        assert_equal({
                       "families_reduced" => {
                         "records" => {
                           "type"       => "sort",
                           "operators"  => [],
                           "key_column" => 1,
                           "limit"      => 1,
                         },
                       },
                     },
                     reduce_message["body"]["families"])
      end

      def test_gather_records
        assert_equal({
                       "elements" => {
                         "records" => {
                           "attributes" => ["count"],
                           "limit"      => 1,
                         },
                       },
                       "output" => "families",
                     },
                     gather_message["body"]["families_reduced"])
      end
    end

    class SubRecodsTest < self
      def setup
        @output = {
          "elements"   => ["records"],
          "attributes" => [
            "_key",
            "_nsubrecs",
            { "label" => "users",
              "source" => "_subrecs",
              "attributes" => ["_key"] },
          ],
          "limit"      => 1,
        }
        @group_by = {
          "key"            => "family_name",
          "maxNSubRecords" => 3,
        }
        @request = {
          "type"    => "search",
          "dataset" => "Droonga",
          "body"    => {
            "queries" => {
              "families" => {
                "source"  => "User",
                "groupBy" => @group_by,
                "output"  => @output,
              },
            },
          },
        }
      end

      def test_dependencies
        reduce_inputs = ["errors", "families"]
        gather_inputs = ["errors_reduced", "families_reduced"]
        assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                     dependencies)
      end

      def test_broadcast_body
        changed_output_parameters = {
          "unifiable"  => true,
        }
        assert_equal({
                       "queries" => {
                         "families" => {
                           "source"  => "User",
                           "groupBy" => @group_by,
                           "output"  => @output.merge(changed_output_parameters),
                         },
                       },
                     },
                     broadcast_message["body"])
      end

      def test_reduce_body
        assert_equal({
                       "families_reduced" => {
                         "records" => {
                           "type"       => "sort",
                           "operators"  => [],
                           "key_column" => 0,
                           "limit"      => 1,
                         },
                       },
                     },
                     reduce_message["body"]["families"])
      end

      def test_gather_records
        assert_equal({
                       "elements" => {
                         "records" => {
                           "attributes" => ["_key", "_nsubrecs", "users"],
                           "limit"      => 1,
                         },
                       },
                       "output" => "families",
                     },
                     gather_message["body"]["families_reduced"])
      end
    end

    class CountOnlyTest < self
      def setup
        @output = {
          "elements" => ["count"],
        }
        @group_by = "family_name"
        @request = {
          "type"    => "search",
          "dataset" => "Droonga",
          "body"    => {
            "queries" => {
              "families" => {
                "source"  => "User",
                "groupBy" => @group_by,
                "output"  => @output,
              },
            },
          },
        }
      end

      def test_dependencies
        reduce_inputs = ["errors", "families"]
        gather_inputs = ["errors_reduced", "families_reduced"]
        assert_equal(expected_dependencies(reduce_inputs, gather_inputs),
                     dependencies)
      end

      def test_broadcast_body
        changed_output_parameters = {
          "elements"   => ["count", "records"],
          "attributes" => ["_key"],
          "limit"      => -1,
          "unifiable"  => true,
        }
        assert_equal({
                       "queries" => {
                         "families" => {
                           "source"  => "User",
                           "groupBy" => @group_by,
                           "output"  => @output.merge(changed_output_parameters),
                         },
                       },
                     },
                     broadcast_message["body"])
      end

      def test_reduce_body
        assert_equal({
                       "families_reduced" => {
                         "count"   => {
                           "type" => "sum",
                         },
                         "records" => {
                           "type"       => "sort",
                           "operators"  => [],
                           "key_column" => 0,
                           "limit"      => -1,
                         },
                       },
                     },
                     reduce_message["body"]["families"])
      end

      def test_gather_records
        assert_equal({
                       "elements" => {
                         "count" => {
                           "target" => "records",
                         },
                         "records" => {
                           "no_output" => true,
                         },
                       },
                       "output" => "families",
                     },
                     gather_message["body"]["families_reduced"])
      end
    end
  end
end
