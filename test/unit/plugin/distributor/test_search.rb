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

  class MultipleQueriesTest < SearchDistributorTest
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
          "query1_reduced" => {
            "source" => "query1",
            "element" => "records",
            "offset" => 0,
            "limit" => 10,
            "format" => "complex",
            "attributes" => [],
          },
          "query2_reduced" => {
            "source" => "query2",
            "element" => "records",
            "offset" => 0,
            "limit" => 20,
            "format" => "complex",
            "attributes" => [],
          },
          "query3_reduced" => {
            "source" => "query3",
            "element" => "records",
            "offset" => 0,
            "limit" => 30,
            "format" => "complex",
            "attributes" => [],
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

      assert_equal(message, @posted.last.last)
    end
  end

  class SingleQueryTest < SearchDistributorTest
    def test_no_output
      envelope = {
        "type" => "search",
        "dataset" => "Droonga",
        "body" => {
          "queries" => {
            "no_output" => {
              "source" => "User",
              "sortBy" => {
                "keys" => ["name"],
                "offset" => 0,
                "limit" => 1,
              },
            },
          },
        },
      }

      @plugin.process("search", envelope)

      message = []
      message << gatherer(envelope, :no_output => true)
      message << searcher(envelope, :no_output => true)
      assert_equal(message, @posted.last.last)
    end

    def test_no_records_element
      envelope = {
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

      @plugin.process("search", envelope)

      message = []
      message << reducer(envelope, {
        "count" => {
          "type" => "sum",
        },
      })
      message << gatherer(envelope)
      message << searcher(envelope, :output_limit => 0)
      assert_equal(message, @posted.last.last)
    end

    def test_no_output_limit
      envelope = {
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

      @plugin.process("search", envelope)

      message = []
      message << reducer(envelope, {
        "count" => {
          "type" => "sum",
        },
      })
      message << gatherer(envelope)
      message << searcher(envelope, :output_offset => 0,
                                    :output_limit => 0)
      assert_equal(message, @posted.last.last)
    end

    def test_have_records
      envelope = {
        "type" => "search",
        "dataset" => "Droonga",
        "body" => {
          "queries" => {
            "have_records" => {
              "source" => "User",
              "output" => {
                "format" => "complex",
                "elements" => ["count", "records"],
                "attributes" => ["_key", "name", "age"],
                "offset" => 0,
                "limit" => 1,
              },
            },
          },
        },
      }

      @plugin.process("search", envelope)

      message = []
      message << reducer(envelope, {
        "count" => {
          "type" => "sum",
        },
        "records" => {
          "type" => "sort",
          "order" => ["<"],
          "limit" => 1,
        },
      })
      message << gatherer(envelope, :offset => 0,
                                    :limit => 1,
                                    :element => "records",
                                    :format => "complex",
                                    :attributes => ["_key", "name", "age"])
      message << searcher(envelope, :output_offset => 0,
                                    :output_limit => 1)
      assert_equal(message, @posted.last.last)
    end

    def test_have_output_offset
      envelope = {
        "type" => "search",
        "dataset" => "Droonga",
        "body" => {
          "queries" => {
            "have_records" => {
              "source" => "User",
              "output" => {
                "format" => "complex",
                "elements" => ["count", "records"],
                "attributes" => ["_key", "name", "age"],
                "offset" => 1,
                "limit" => 1,
              },
            },
          },
        },
      }

      @plugin.process("search", envelope)

      message = []
      message << reducer(envelope, {
        "count" => {
          "type" => "sum",
        },
        "records" => {
          "type" => "sort",
          "order" => ["<"],
          "limit" => 2,
        },
      })
      message << gatherer(envelope, :offset => 1,
                                    :limit => 1,
                                    :element => "records",
                                    :format => "complex",
                                    :attributes => ["_key", "name", "age"])
      message << searcher(envelope, :output_offset => 0,
                                    :output_limit => 2)
      assert_equal(message, @posted.last.last)
    end

    def test_have_simple_sortBy
      envelope = {
        "type" => "search",
        "dataset" => "Droonga",
        "body" => {
          "queries" => {
            "have_records" => {
              "source" => "User",
              "sortBy" => ["name"],
              "output" => {
                "format" => "complex",
                "elements" => ["count", "records"],
                "attributes" => ["_key", "name", "age"],
                "offset" => 0,
                "limit" => 1,
              },
            },
          },
        },
      }

      @plugin.process("search", envelope)

      message = []
      message << reducer(envelope, {
        "count" => {
          "type" => "sum",
        },
        "records" => {
          "type" => "sort",
          "order" => ["<"],
          "limit" => 1,
        },
      })
      message << gatherer(envelope, :offset => 0,
                                    :limit => 1,
                                    :element => "records",
                                    :format => "complex",
                                    :attributes => ["_key", "name", "age"])
      message << searcher(envelope, :output_offset => 0,
                                    :output_limit => 1)
      assert_equal(message, @posted.last.last)
    end

    def test_have_sortBy
      envelope = {
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
                "elements" => ["count", "records"],
                "attributes" => ["_key", "name", "age"],
                "offset" => 0,
                "limit" => 1,
              },
            },
          },
        },
      }

      @plugin.process("search", envelope)

      message = []
      message << reducer(envelope, {
        "count" => {
          "type" => "sum",
        },
        "records" => {
          "type" => "sort",
          "order" => ["<"],
          "limit" => 1,
        },
      })
      message << gatherer(envelope, :offset => 0,
                                    :limit => 1,
                                    :element => "records",
                                    :format => "complex",
                                    :attributes => ["_key", "name", "age"])
      message << searcher(envelope, :sort_offset => 0,
                                    :sort_limit => 1,
                                    :output_offset => 0,
                                    :output_limit => 1)
      assert_equal(message, @posted.last.last)
    end

    def test_have_sortBy_offset_limit
      envelope = {
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
                "elements" => ["count", "records"],
                "attributes" => ["_key", "name", "age"],
                "offset" => 4,
                "limit" => 8,
              },
            },
          },
        },
      }

      @plugin.process("search", envelope)

      message = []
      message << reducer(envelope, {
        "count" => {
          "type" => "sum",
        },
        "records" => {
          "type" => "sort",
          "order" => ["<"],
          "limit" => 1 + 4 + [2, 8].min,
        },
      })
      message << gatherer(envelope, :offset => 5,
                                    :limit => 2,
                                    :element => "records",
                                    :format => "complex",
                                    :attributes => ["_key", "name", "age"])
      message << searcher(envelope, :sort_offset => 0,
                                    :sort_limit => 7,
                                    :output_offset => 0,
                                    :output_limit => 7)
      assert_equal(message, @posted.last.last)
    end

    def test_have_sortBy_with_infinity_output_limit
      envelope = {
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
                "elements" => ["count", "records"],
                "attributes" => ["_key", "name", "age"],
                "offset" => 4,
                "limit" => -1,
              },
            },
          },
        },
      }

      @plugin.process("search", envelope)

      message = []
      message << reducer(envelope, {
        "count" => {
          "type" => "sum",
        },
        "records" => {
          "type" => "sort",
          "order" => ["<"],
          "limit" => 1 + 4 + 2,
        },
      })
      message << gatherer(envelope, :offset => 5,
                                    :limit => 2,
                                    :element => "records",
                                    :format => "complex",
                                    :attributes => ["_key", "name", "age"])
      message << searcher(envelope, :sort_offset => 0,
                                    :sort_limit => 7,
                                    :output_offset => 0,
                                    :output_limit => 7)
      assert_equal(message, @posted.last.last)
    end

    def test_have_sortBy_with_infinity_sort_limit
      envelope = {
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
                "elements" => ["count", "records"],
                "attributes" => ["_key", "name", "age"],
                "offset" => 4,
                "limit" => 8,
              },
            },
          },
        },
      }

      @plugin.process("search", envelope)

      message = []
      message << reducer(envelope, {
        "count" => {
          "type" => "sum",
        },
        "records" => {
          "type" => "sort",
          "order" => ["<"],
          "limit" => 1 + 4 + 8,
        },
      })
      message << gatherer(envelope, :offset => 5,
                                    :limit => 8,
                                    :element => "records",
                                    :format => "complex",
                                    :attributes => ["_key", "name", "age"])
      message << searcher(envelope, :sort_offset => 0,
                                    :sort_limit => 8,
                                    :output_offset => 0,
                                    :output_limit => 8)
      assert_equal(message, @posted.last.last)
    end

    def test_have_sortBy_with_infinity_limit
      envelope = {
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
                "elements" => ["count", "records"],
                "attributes" => ["_key", "name", "age"],
                "offset" => 4,
                "limit" => -1,
              },
            },
          },
        },
      }

      @plugin.process("search", envelope)

      message = []
      message << reducer(envelope, {
        "count" => {
          "type" => "sum",
        },
        "records" => {
          "type" => "sort",
          "order" => ["<"],
          "limit" => -1,
        },
      })
      message << gatherer(envelope, :offset => 5,
                                    :limit => -1,
                                    :element => "records",
                                    :format => "complex",
                                    :attributes => ["_key", "name", "age"])
      message << searcher(envelope, :sort_offset => 0,
                                    :sort_limit => -1,
                                    :output_offset => 0,
                                    :output_limit => -1)
      assert_equal(message, @posted.last.last)
    end

    private
    def reducer(search_request_envelope, reducer_body)
      queries = search_request_envelope["body"]["queries"]
      query_name = queries.keys.first

      reducer = {
        "type" => "reduce",
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

    def gatherer(search_request_envelope, options={})
      queries = search_request_envelope["body"]["queries"]
      query_name = queries.keys.first

      gatherer = {
        "type" => "gather",
        "body" => {
        },
        "inputs" => [
        ],
        "post" => true,
      }

      unless options[:no_output]
        output = {
          "source" => query_name,
        }
        if options[:element]
          output.merge!({
            "element" => options[:element],
            "offset" => options[:offset] || 0,
            "limit" => options[:limit] || 0,
            "format" => options[:format] || "simple",
            "attributes" => options[:attributes] || [],
          })
        end
        gatherer["body"]["#{query_name}_reduced"] = output
        gatherer["inputs"] << "#{query_name}_reduced"
      end

      gatherer
    end

    def searcher(search_request_envelope, options={})
      searcher = search_request_envelope.dup

      queries = searcher["body"]["queries"]
      query_name = queries.keys.first
      if options[:sort_offset]
        queries[query_name]["sortBy"]["offset"] = options[:sort_offset]
      end
      if options[:sort_limit]
        queries[query_name]["sortBy"]["limit"] = options[:sort_limit]
      end
      if options[:output_offset]
        queries[query_name]["output"]["offset"] = options[:output_offset]
      end
      if options[:output_limit]
        queries[query_name]["output"]["limit"] = options[:output_limit]
      end

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
