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

class DistributedSearchPlanTest < Test::Unit::TestCase
  def assert_planned(expected, search_request_envelope)
    plan = Droonga::DistributedSearchPlan.new(search_request_envelope)
    actual = plan.messages
    assert_equal(expected, actual)
  end

  class MultipleQueriesTest < DistributedSearchPlanTest
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
        "type" => "reduce",
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
        "type" => "reduce",
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
        "type" => "gather",
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

      assert_planned(message, envelope)
    end
  end

  class SingleQueryTest < DistributedSearchPlanTest
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

      message = []
      message << gatherer(envelope, :no_output => true)
      message << searcher(envelope, :no_output => true)
      assert_planned(message, envelope)
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

      message = []
      message << reducer(envelope, {
        "count" => {
          "type" => "sum",
        },
      })
      message << gatherer(envelope)
      message << searcher(envelope, :sort_limit => 0,
                                    :output_limit => 0)
      assert_planned(message, envelope)
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

      message = []
      message << reducer(envelope, {
        "count" => {
          "type" => "sum",
        },
      })
      message << gatherer(envelope)
      message << searcher(envelope, :output_offset => 0,
                                    :output_limit => 0)
      assert_planned(message, envelope)
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
                "elements" => ["records"],
                "attributes" => ["_key", "name", "age"],
                "offset" => 0,
                "limit" => 1,
              },
            },
          },
        },
      }

      message = []
      message << reducer(envelope, {
        "records" => {
          "type" => "sort",
          "operators" => [],
          "limit" => 1,
        },
      })
      message << gatherer(envelope, :elements => {
                                      "records" => records_mapper(
                                        :offset => 0,
                                        :limit => 1,
                                        :format => "complex",
                                        :attributes => ["_key", "name", "age"],
                                      ),
                                    })
      message << searcher(envelope, :output_offset => 0,
                                    :output_limit => 1)
      assert_planned(message, envelope)
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
                "elements" => ["records"],
                "attributes" => ["_key", "name", "age"],
                "offset" => 1,
                "limit" => 1,
              },
            },
          },
        },
      }

      message = []
      message << reducer(envelope, {
        "records" => {
          "type" => "sort",
          "operators" => [],
          "limit" => 2,
        },
      })
      message << gatherer(envelope, :elements => {
                                      "records" => records_mapper(
                                        :offset => 1,
                                        :limit => 1,
                                        :format => "complex",
                                        :attributes => ["_key", "name", "age"],
                                      ),
                                    })
      message << searcher(envelope, :output_offset => 0,
                                    :output_limit => 2)
      assert_planned(message, envelope)
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
                "elements" => ["records"],
                "attributes" => ["_key", "name", "age"],
                "offset" => 0,
                "limit" => 1,
              },
            },
          },
        },
      }

      message = []
      message << reducer(envelope, {
        "records" => {
          "type" => "sort",
          "operators" => [
            { "column" => 1, "operator" => "<" },
          ],
          "limit" => 1,
        },
      })
      message << gatherer(envelope, :elements => {
                                      "records" => records_mapper(
                                        :offset => 0,
                                        :limit => 1,
                                        :format => "complex",
                                        :attributes => ["_key", "name", "age"],
                                      ),
                                    })
      message << searcher(envelope, :output_offset => 0,
                                    :output_limit => 1)
      assert_planned(message, envelope)
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
                "elements" => ["records"],
                "attributes" => ["_key", "name", "age"],
                "offset" => 0,
                "limit" => 1,
              },
            },
          },
        },
      }

      message = []
      message << reducer(envelope, {
        "records" => {
          "type" => "sort",
          "operators" => [
            { "column" => 1, "operator" => "<" },
          ],
          "limit" => 1,
        },
      })
      message << gatherer(envelope, :elements => {
                                      "records" => records_mapper(
                                        :offset => 0,
                                        :limit => 1,
                                        :format => "complex",
                                        :attributes => ["_key", "name", "age"],
                                      ),
                                    })
      message << searcher(envelope, :sort_offset => 0,
                                    :sort_limit => 1,
                                    :output_offset => 0,
                                    :output_limit => 1)
      assert_planned(message, envelope)
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
                "elements" => ["records"],
                "attributes" => ["_key", "name", "age"],
                "offset" => 4,
                "limit" => 8,
              },
            },
          },
        },
      }

      limit = 1 + 4 + [2, 8].min
      message = []
      message << reducer(envelope, {
        "records" => {
          "type" => "sort",
          "operators" => [
            { "column" => 1, "operator" => "<" },
          ],
          "limit" => limit,
        },
      })
      message << gatherer(envelope, :elements => {
                                      "records" => records_mapper(
                                        :offset => 5,
                                        :limit => 2,
                                        :format => "complex",
                                        :attributes => ["_key", "name", "age"],
                                      ),
                                    })
      message << searcher(envelope, :sort_offset => 0,
                                    :sort_limit => limit,
                                    :output_offset => 0,
                                    :output_limit => limit)
      assert_planned(message, envelope)
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
      message = []
      message << reducer(envelope, {
        "records" => {
          "type" => "sort",
          "operators" => [
            { "column" => 1, "operator" => "<" },
          ],
          "limit" => limit,
        },
      })
      message << gatherer(envelope, :elements => {
                                      "records" => records_mapper(
                                        :offset => 5,
                                        :limit => 2,
                                        :format => "complex",
                                        :attributes => ["_key", "name", "age"],
                                      ),
                                    })
      message << searcher(envelope, :sort_offset => 0,
                                    :sort_limit => limit,
                                    :output_offset => 0,
                                    :output_limit => limit)
      assert_planned(message, envelope)
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
      message = []
      message << reducer(envelope, {
        "records" => {
          "type" => "sort",
          "operators" => [
            { "column" => 1, "operator" => "<" },
          ],
          "limit" => limit,
        },
      })
      message << gatherer(envelope, :elements => {
                                      "records" => records_mapper(
                                        :offset => 5,
                                        :limit => 8,
                                        :format => "complex",
                                        :attributes => ["_key", "name", "age"],
                                      ),
                                    })
      message << searcher(envelope, :sort_offset => 0,
                                    :sort_limit => limit,
                                    :output_offset => 0,
                                    :output_limit => limit)
      assert_planned(message, envelope)
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
                "elements" => ["records"],
                "attributes" => ["_key", "name", "age"],
                "offset" => 4,
                "limit" => -1,
              },
            },
          },
        },
      }

      message = []
      message << reducer(envelope, {
        "records" => {
          "type" => "sort",
          "operators" => [
            { "column" => 1, "operator" => "<" },
          ],
          "limit" => -1,
        },
      })
      message << gatherer(envelope, :elements => {
                                      "records" => records_mapper(
                                        :offset => 5,
                                        :limit => -1,
                                        :format => "complex",
                                        :attributes => ["_key", "name", "age"],
                                      ),
                                    })
      message << searcher(envelope, :sort_offset => 0,
                                    :sort_limit => -1,
                                    :output_offset => 0,
                                    :output_limit => -1)
      assert_planned(message, envelope)
    end

    def test_have_sortBy_with_multiple_sort_keys
      envelope = {
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

      message = []
      message << reducer(envelope, {
        "records" => {
          "type" => "sort",
          "operators" => [
            { "column" => 2, "operator" => ">" },
            { "column" => 1, "operator" => "<" },
          ],
          "limit" => -1,
        },
      })
      message << gatherer(envelope, :elements => {
                                      "records" => records_mapper(
                                        :offset => 0,
                                        :limit => -1,
                                        :format => "complex",
                                        :attributes => ["_key", "name", "age"],
                                      ),
                                    })
      message << searcher(envelope, :sort_offset => 0,
                                    :sort_limit => -1,
                                    :output_offset => 0,
                                    :output_limit => -1)
      assert_planned(message, envelope)
    end

    def test_have_sortBy_with_missing_sort_attributes
      envelope = {
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

      message = []
      message << reducer(envelope, {
        "records" => {
          "type" => "sort",
          "operators" => [
            { "column" => 3, "operator" => ">" },
            { "column" => 4, "operator" => "<" },
          ],
          "limit" => -1,
        },
      })
      message << gatherer(envelope, :elements => {
                                      "records" => records_mapper(
                                        :offset => 0,
                                        :limit => -1,
                                        :format => "complex",
                                        :attributes => ["_key", "name", "age"],
                                      ),
                                    })
      message << searcher(envelope, :sort_offset => 0,
                                    :sort_limit => -1,
                                    :output_offset => 0,
                                    :output_limit => -1,
                                    :extra_attributes => ["public_age", "public_name"])
      assert_planned(message, envelope)
    end

    def test_hash_attributes
      envelope = {
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

      message = []
      message << reducer(envelope, {
        "records" => {
          "type" => "sort",
          "operators" => [
            { "column" => 3, "operator" => ">" },
            { "column" => 4, "operator" => "<" },
          ],
          "limit" => -1,
        },
      })
      message << gatherer(envelope, :elements => {
                                      "records" => records_mapper(
                                        :offset => 0,
                                        :limit => -1,
                                        :format => "complex",
                                        :attributes => ["id", "name", "age"],
                                      ),
                                    })
      message << searcher(envelope, :sort_offset => 0,
                                    :sort_limit => -1,
                                    :output_offset => 0,
                                    :output_limit => -1,
                                    :extra_attributes => ["public_age", "public_name"])
      assert_planned(message, envelope)
    end

    def test_groupBy
      envelope = {
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

      message = []
      message << reducer(envelope, {
        "records" => {
          "type" => "sort",
          "operators" => [],
          "key_column" => 0,
          "limit" => -1,
        },
      })
      message << gatherer(envelope, :elements => {
                                      "records" => records_mapper(
                                        :offset => 0,
                                        :limit => -1,
                                        :format => "complex",
                                        :attributes => ["_key", "_nsubrecs"],
                                      ),
                                    })
      message << searcher(envelope, :output_offset => 0,
                                    :output_limit => -1,
                                    :unifiable => true)
      assert_planned(message, envelope)
    end

    def test_groupBy_count
      envelope = {
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

      message = []
      message << reducer(envelope, {
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
      message << gatherer(envelope, :elements => {
                                      "count" => count_mapper,
                                      "records" => records_mapper(
                                        :limit => -1,
                                        :attributes => ["_key"],
                                        :no_output => true,
                                      ),
                                    })
      message << searcher(envelope, :output_limit => -1,
                                    :extra_attributes => ["_key"],
                                    :extra_elements => ["records"],
                                    :unifiable => true)
      assert_planned(message, envelope)
    end

    def test_groupBy_hash
      envelope = {
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

      message = []
      message << reducer(envelope, {
        "records" => {
          "type" => "sort",
          "operators" => [],
          "key_column" => 3, # 0=family_name, 1=_nsubrecs, 2=_subrecs, 3=_keys
          "limit" => -1,
        },
      })
      message << gatherer(envelope, :elements => {
                                      "records" => records_mapper(
                                        :offset => 0,
                                        :limit => -1,
                                        :format => "complex",
                                        :attributes => ["family_name", "count", "users"],
                                      ),
                                    })
      message << searcher(envelope, :output_offset => 0,
                                    :output_limit => -1,
                                    :extra_attributes => ["_key"],
                                    :unifiable => true)
      assert_planned(message, envelope)
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
          "output" => query_name,
          "elements" => options[:elements] || {},
        }
        gatherer["body"]["#{query_name}_reduced"] = output
        gatherer["inputs"] << "#{query_name}_reduced"
      end

      gatherer
    end

    def searcher(search_request_envelope, options={})
      # dup and clone don't copy it deeply...
      searcher = Marshal.load(Marshal.dump(search_request_envelope))

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
