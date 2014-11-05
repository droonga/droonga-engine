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

class DistributedSearchPlannerBasicTest < Test::Unit::TestCase
  include DistributedSearchPlannerHelper

  class MultipleQueriesTest < self
    class MultipleOutputsTest < self
      def setup
        @request = {
          "type"    => "search",
          "dataset" => "Default",
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
        "dataset" => "Default",
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
end
