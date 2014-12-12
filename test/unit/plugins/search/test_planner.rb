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

require "droonga/plugins/search"

class SearchPlannerTest < Test::Unit::TestCase
  def setup
    setup_database
    @planner = Droonga::Test::StubPlanner.new
    # TODO: Use real dataset
    stub_dataset = Object.new
    stub(stub_dataset).name do
      Droonga::Catalog::Dataset::DEFAULT_NAME
    end
    stub(stub_dataset).sliced? do
      true
    end
    @plugin = Droonga::Plugins::Search::Planner.new(stub_dataset)
  end

  def teardown
    teardown_database
  end

  def test_distribute
    envelope = {
      "type" => "search",
      "dataset" => "Droonga",
      "body" => {
        "queries" => {
          "query1" => {
            "source" => "User",
            "output" => {
              "format" => "complex",
              "elements" => ["count", "records"],
              "attributes" => [],
              "offset" => 0,
              "limit" => 10,
            },
          },
          "query2" => {
            "source" => "User",
            "output" => {
              "format" => "complex",
              "elements" => ["count", "records"],
              "attributes" => [],
              "offset" => 0,
              "limit" => 20,
            },
          },
        },
      },
    }

    @planner.distribute(@plugin.plan(envelope))

    message = []

    message << {
      "type" => "search_reduce",
      "body" => {
        "errors" => {
          "errors_reduced" => {
            "limit" => -1,
            "type"  => "sum",
          },
        },
        "query1" => {
          "query1_reduced" => {
            "count" => {
              "type" => "sum",
            },
            "records" => {
              "type" => "sort",
              "operators" => [],
              "limit" => 10,
            },
          },
        },
        "query2" => {
          "query2_reduced" => {
            "count" => {
              "type" => "sum",
            },
            "records" => {
              "type" => "sort",
              "operators" => [],
              "limit" => 20,
            },
          },
        },
      },
      "inputs" => ["errors", "query1", "query2"],
      "outputs" => ["errors_reduced", "query1_reduced", "query2_reduced"],
    }

    gatherer = {
      "type" => "search_gather",
      "body" => {
        "errors_reduced" => {
          "output" => "errors",
        },
        "query1_reduced" => {
          "output" => "query1",
          "elements" => {
            "records" => {
              "limit" => 10,
              "format" => "complex",
            },
          },
        },
        "query2_reduced" => {
          "output" => "query2",
          "elements" => {
            "records" => {
              "limit" => 20,
              "format" => "complex",
            },
          },
        },
      },
      "inputs" => [
        "errors_reduced",
        "query1_reduced",
        "query2_reduced",
      ],
      "post" => true,
    }
    message << gatherer

    searcher = {
      "type" => "broadcast",
      "command" => "search",
      "dataset" => "Default",
      "body" => {
        "queries" => {
          "query1" => {
            "source" => "User",
            "output" => {
              "format" => "simple",
              "elements" => ["count", "records"],
              "attributes" => [],
              "offset" => 0,
              "limit" => 10,
            },
          },
          "query2" => {
            "source" => "User",
            "output" => {
              "format" => "simple",
              "elements" => ["count", "records"],
              "attributes" => [],
              "offset" => 0,
              "limit" => 20,
            },
          },
        },
      },
      "outputs" => [
        "errors",
        "query1",
        "query2",
      ],
      "replica" => "random",
    }
    message << searcher

    assert_equal([message], @planner.messages)
  end
end
