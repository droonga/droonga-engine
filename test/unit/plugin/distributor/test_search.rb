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

require "droonga/plugin/distributor/search"

class SearchDistributorTest < Test::Unit::TestCase
  def setup
    setup_database
    @distributor = Droonga::Test::StubDistributor.new
    @plugin = Droonga::SearchDistributor.new(@distributor)
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
          "query3" => {
            "source" => "User",
            "output" => {
              "format" => "complex",
              "elements" => ["count", "records"],
              "attributes" => [],
              "offset" => 0,
              "limit" => 30,
            },
          },
        },
      },
    }

    @plugin.process("search", envelope)

    message = []

    message << {
      "type" => "search_reduce",
      "body" => {
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
      },
      "inputs" => ["query1"],
      "outputs" => ["query1_reduced"],
    }
    message << {
      "type" => "search_reduce",
      "body" => {
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
      "inputs" => ["query2"],
      "outputs" => ["query2_reduced"],
    }
    message << {
      "type" => "search_reduce",
      "body" => {
        "query3" => {
          "query3_reduced" => {
            "count" => {
              "type" => "sum",
            },
            "records" => {
              "type" => "sort",
              "operators" => [],
              "limit" => 30,
            },
          },
        },
      },
      "inputs" => ["query3"],
      "outputs" => ["query3_reduced"],
    }

    gatherer = {
      "type" => "search_gather",
      "body" => {
        "query1_reduced" => {
          "output" => "query1",
          "elements" => {
            "records" => {
              "type" => "sort",
              "offset" => 0,
              "limit" => 10,
              "format" => "complex",
              "attributes" => [],
            },
          },
        },
        "query2_reduced" => {
          "output" => "query2",
          "elements" => {
            "records" => {
              "type" => "sort",
              "offset" => 0,
              "limit" => 20,
              "format" => "complex",
              "attributes" => [],
            },
          },
        },
        "query3_reduced" => {
          "output" => "query3",
          "elements" => {
            "records" => {
              "type" => "sort",
              "offset" => 0,
              "limit" => 30,
              "format" => "complex",
              "attributes" => [],
            },
          },
        },
      },
      "inputs" => [
        "query1_reduced",
        "query2_reduced",
        "query3_reduced",
      ],
      "post" => true,
    }
    message << gatherer

    searcher = {
      "type" => "broadcast",
      "command" => "search",
      "dataset" => "Droonga",
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
          "query3" => {
            "source" => "User",
            "output" => {
              "format" => "simple",
              "elements" => ["count", "records"],
              "attributes" => [],
              "offset" => 0,
              "limit" => 30,
            },
          },
        },
      },
      "outputs" => [
        "query1",
        "query2",
        "query3",
      ],
      "replica" => "random",
    }
    message << searcher

    assert_equal([message], @distributor.messages)
  end
end
