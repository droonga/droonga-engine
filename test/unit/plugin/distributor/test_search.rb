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
  include PluginHelper

  def setup
    setup_database
    setup_plugin(Droonga::SearchDistributor)
  end

  def teardown
    teardown_plugin
    teardown_database
  end

  def test_multiple_queries
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
              "offset" => 0,
              "limit" => 10,
            },
          },
          "query2" => {
            "source" => "User",
            "output" => {
              "format" => "complex",
              "elements" => ["count", "records"],
              "offset" => 0,
              "limit" => 20,
            },
          },
          "query3" => {
            "source" => "User",
            "output" => {
              "format" => "complex",
              "elements" => ["count", "records"],
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
      "type" => "reduce",
      "body" => {
        "query1" => {
          "query1_reduced" => {
            "count" => {
              "type" => "sum",
            },
            "records" => {
              "type" => "sort",
              "order" => ["<"],
              "offset" => 0,
              "limit" => 10,
            },
          },
        },
      },
      "inputs" => ["query1"],
      "outputs" => ["query1_reduced"],
    }
    message << {
      "type" => "reduce",
      "body" => {
        "query2" => {
          "query2_reduced" => {
            "count" => {
              "type" => "sum",
            },
            "records" => {
              "type" => "sort",
              "order" => ["<"],
              "offset" => 0,
              "limit" => 20,
            },
          },
        },
      },
      "inputs" => ["query2"],
      "outputs" => ["query2_reduced"],
    }
    message << {
      "type" => "reduce",
      "body" => {
        "query3" => {
          "query3_reduced" => {
            "count" => {
              "type" => "sum",
            },
            "records" => {
              "type" => "sort",
              "order" => ["<"],
              "offset" => 0,
              "limit" => 30,
            },
          },
        },
      },
      "inputs" => ["query3"],
      "outputs" => ["query3_reduced"],
    }

    gatherer = {
      "type" => "gather",
      "body" => {
        "query1_reduced" => "query1",
        "query2_reduced" => "query2",
        "query3_reduced" => "query3",
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
              "format" => "complex",
              "elements" => ["count", "records"],
              "offset" => 0,
              "limit" => 10,
            },
          },
          "query2" => {
            "source" => "User",
            "output" => {
              "format" => "complex",
              "elements" => ["count", "records"],
              "offset" => 0,
              "limit" => 20,
            },
          },
          "query3" => {
            "source" => "User",
            "output" => {
              "format" => "complex",
              "elements" => ["count", "records"],
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
    assert_equal(message, @posted.last.last)
  end

  def test_distribute
    envelope = {
      "type" => "search",
      "dataset" => "Droonga",
      "body" => {
        "queries" => {
          "no_output" => {
            "source" => "User",
          },
          "no_records" => {
            "source" => "User",
            "output" => {
              "elements" => ["count"],
            },
          },
          "no_limit" => {
            "source" => "User",
            "output" => {
              "format" => "complex",
              "elements" => ["count", "records"],
            },
          },
          "have_records" => {
            "source" => "User",
            "output" => {
              "format" => "complex",
              "elements" => ["count", "records"],
              "attributes" => ["_key", "name", "age"],
              "offset" => 1,
              "limit" => 2,
            },
          },
          # XXX we should write cases for...
          #  - sortBy(simple)
          #  - sortBy(rich)
          #  - sortBy(rich) with offset
          #  - sortBy(rich) with limit
          #  - sortBy(rich) with offset and limit
          #  - sortBy(simple) + output(limit, offset)
          #  - sortBy(rich)
          #    + output(limit, offset)
          #  - sortBy(rich) with offset
          #    + output(limit, offset)
          #  - sortBy(rich) with limit
          #    + output(limit, offset)
          #  - sortBy(rich) with offset and limit
          #    + output(limit, offset)
          # and, we have to write cases for both unlimited and limited cases...
        },
      },
    }

    @plugin.process("search", envelope)

    message = []
    no_records_reducer = {
      "type" => "reduce",
      "body" => {
        "no_records" => {
          "no_records_reduced" => {
            "count" => {
              "type" => "sum",
            },
          },
        },
      },
      "inputs" => ["no_records"],
      "outputs" => ["no_records_reduced"],
    }
    message << no_records_reducer
    no_limit_reducer = {
      "type" => "reduce",
      "body" => {
        "no_limit" => {
          "no_limit_reduced" => {
            "count" => {
              "type" => "sum",
            },
            "records" => {
              "type" => "sort",
              "order" => ["<"],
              "offset" => 0,
              "limit" => 0,
            },
          },
        },
      },
      "inputs" => ["no_limit"],
      "outputs" => ["no_limit_reduced"],
    }
    message << no_limit_reducer
    have_records_reducer = {
      "type" => "reduce",
      "body" => {
        "have_records" => {
          "have_records_reduced" => {
            "count" => {
              "type" => "sum",
            },
            "records" => {
              "type" => "sort",
              "order" => ["<"],
              "offset" => 1,
              "limit" => 2,
            },
          },
        },
      },
      "inputs" => ["have_records"],
      "outputs" => ["have_records_reduced"],
    }
    message << have_records_reducer

    gatherer = {
      "type" => "gather",
      "body" => {
        "no_records_reduced" => "no_records",
        "no_limit_reduced" => "no_limit",
        "have_records_reduced" => "have_records",
      },
      "inputs" => [
        "no_records_reduced",
        "no_limit_reduced",
        "have_records_reduced",
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
          "no_output" => {
            "source" => "User",
          },
          "no_records" => {
            "source" => "User",
            "output" => {
              "elements" => ["count"],
              "limit" => 0,
            },
          },
          "no_limit" => {
            "source" => "User",
            "output" => {
              "format" => "complex",
              "elements" => ["count", "records"],
              "offset" => 0,
              "limit" => 0,
            },
          },
          "have_records" => {
            "source" => "User",
            "output" => {
              "format" => "complex",
              "elements" => ["count", "records"],
              "attributes" => ["_key", "name", "age"],
              "offset" => 0,
              "limit" => 3,
            },
          },
        },
      },
      "outputs" => [
        "no_records",
        "no_limit",
        "have_records",
      ],
      "replica" => "random",
    }
    message << searcher
    assert_equal(message, @posted.last.last)
  end
end
