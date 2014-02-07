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

class DistributedSearchPlannerOutputTest < Test::Unit::TestCase
  include DistributedSearchPlannerHelper

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

  class NoLimitTest < self
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

  class OffsetTest < self
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
