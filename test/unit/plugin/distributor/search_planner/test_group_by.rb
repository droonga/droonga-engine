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

class DistributedSearchPlannerGroupByTest < Test::Unit::TestCase
  include DistributedSearchPlannerHelper

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
