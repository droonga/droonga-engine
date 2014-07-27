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

class DistributedSearchPlannerSortByTest < Test::Unit::TestCase
  include DistributedSearchPlannerHelper

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
