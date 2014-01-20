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

require "droonga/plugin/handler/search"

class SearchHandlerTest < Test::Unit::TestCase
  def setup
    setup_database
    setup_data
    setup_plugin
  end

  def teardown
    teardown_plugin
    teardown_database
  end

  def setup_plugin
    @handler = Droonga::Test::StubHandler.new
    @plugin = Droonga::SearchHandler.new(@handler)
    @messenger = Droonga::Test::StubHandlerMessenger.new
  end

  def teardown_plugin
    @handler = nil
    @plugin = nil
  end

  private
  def search(request, headers={})
    message = Droonga::Test::StubHandlerMessage.new(request, headers)
    @plugin.search(message, @messenger)
    results_to_result_set(@messenger.values.first)
  end

  def results_to_result_set(results)
    result_set = {}
    results.each do |name, result|
      result_set[name] = normalize_result(result)
    end
    result_set
  end

  def normalize_result(result)
    result["startTime"] = start_time if result["startTime"]
    result["elapsedTime"] = elapsed_time if result["elapsedTime"]
    result
  end

  def start_time
    "2013-01-31T14:34:47+09:00"
  end

  def elapsed_time
    0.01
  end

  def assert_search(expected, request)
    assert_equal(expected, search(request))
  end

  class GeneralTest < self
    def setup_data
      restore(fixture_data("document.grn"))
    end

    class NoParameterTest < self
      def test_empty
        assert_search({}, {})
      end
    end

    class QueriesTest < self
      def test_empty
        assert_search({}, {"queries" => {}})
      end
    end

    class HashQueryTest < self
      def test_string_matchTo
        request = base_request
        request["queries"]["sections-result"]["condition"] = {
          "query" => "Groonga",
          "matchTo" => "title"
        }
        assert_search({
                        "sections-result" => {
                          "records" => [
                            { "title" => "Groonga overview" },
                          ],
                        },
                      },
                      request)
      end

      def test_array_matchTo
        request = base_request
        request["queries"]["sections-result"]["condition"] = {
          "query" => "Groonga",
          "matchTo" => ["title"]
        }
        assert_search({
                        "sections-result" => {
                          "records" => [
                            { "title" => "Groonga overview" },
                          ],
                        },
                      },
                      request)
      end

      def base_request
        {
          "queries" => {
            "sections-result" => {
              "source" => "Sections",
              "output" => {
                "elements" => [
                  "records",
                ],
                "format" => "complex",
                "limit" => 1,
                "attributes" => ["title"],
              },
            },
          },
        }
      end
    end

    class SourceTest < self
      def test_non_existent
        assert_raise(Droonga::Searcher::UnknownSource) do
          search({
                   "queries" => {
                     "non-existent-result" => {
                       "source" => "non-existent",
                     },
                   },
                 })
        end
      end

      def test_existent
        assert_search({
                        "sections-result" => {},
                      },
                      {
                        "queries" => {
                          "sections-result" => {
                            "source" => "Sections",
                            "output" => {},
                          },
                        },
                      })
      end

      def test_no_source
        assert_raise(Droonga::Searcher::MissingSourceParameter) do
          search({
                   "queries" => {
                     "no-source-result" => {
                     },
                   },
                 })
        end
      end
    end

    class OutputTest < self
      def test_count
        assert_search({
                        "sections-result" => {
                          "count" => 9,
                        },
                      },
                      {
                        "queries" => {
                          "sections-result" => {
                            "source" => "Sections",
                            "output" => {
                              "elements" => [
                                "count",
                              ],
                            },
                          },
                        },
                      })
      end

      def test_elapsed_time
        assert_search({
                        "sections-result" => {
                          "startTime" => start_time,
                          "elapsedTime" => elapsed_time,
                        },
                      },
                      {
                        "queries" => {
                          "sections-result" => {
                            "source" => "Sections",
                            "output" => {
                              "elements" => [
                                "startTime",
                                "elapsedTime",
                              ],
                            },
                          },
                        },
                      })
      end

      def test_attributes_simple
        assert_search({
                        "sections-result" => {
                          "attributes" => [
                            {
                              "name" => "key",
                              "type" => "ShortText",
                              "vector" => false
                            },
                            {
                              "name" => "title",
                              "type" => "ShortText",
                              "vector" => false
                            }
                          ]
                        },
                      },
                      {
                        "queries" => {
                          "sections-result" => {
                            "source" => "Sections",
                            "output" => {
                              "elements" => [
                                "attributes"
                              ],
                              "attributes" => [
                                {
                                  "label" => "key",
                                  "source" => "_key",
                                },
                                "title",
                              ],
                            },
                          },
                        },
                      })
      end

      def test_attributes_complex
        assert_search({
                        "sections-result" => {
                          "attributes" => {
                            "key" => {
                              "type" => "ShortText",
                              "vector" => false
                            },
                            "title" => {
                              "type" => "ShortText",
                              "vector" => false
                            }
                          }
                        },
                      },
                      {
                        "queries" => {
                          "sections-result" => {
                            "source" => "Sections",
                            "output" => {
                              "format" => "complex",
                              "elements" => [
                                "attributes"
                              ],
                              "attributes" => [
                                {
                                  "label" => "key",
                                  "source" => "_key",
                                },
                                "title",
                              ],
                            },
                          },
                        },
                      })
      end

      # TODO test_attributes_complex

      class AttributesTest < self
        def test_source_only
          expected = {
            "sections-result" => {
              "records" => [
                {
                  "_key" => "1.1",
                  "title" => "Groonga overview",
                },
                {
                  "_key" => "1.2",
                  "title" => "Full text search and Instant update",
                },
                {
                  "_key" => "1.3",
                  "title" => "Column store and aggregate query",
                },
              ],
            },
          }
          request = {
            "queries" => {
              "sections-result" => {
                "source" => "Sections",
                "output" => {
                  "elements" => [
                    "records",
                  ],
                  "format" => "complex",
                  "limit" => 3,
                  "attributes" => ["_key", "title"],
                },
              },
            },
          }
          assert_search(expected, request)
        end

        def test_label
          expected = {
            "sections-result" => {
              "records" => [
                {
                  "key" => "1.1",
                  "title" => "Groonga overview",
                },
                {
                  "key" => "1.2",
                  "title" => "Full text search and Instant update",
                },
                {
                  "key" => "1.3",
                  "title" => "Column store and aggregate query",
                },
              ],
            },
          }
          request = {
            "queries" => {
              "sections-result" => {
                "source" => "Sections",
                "output" => {
                  "elements" => [
                    "records",
                  ],
                  "format" => "complex",
                  "limit" => 3,
                  "attributes" => [
                    {
                      "label" => "key",
                      "source" => "_key",
                    },
                    "title",
                  ],
                },
              },
            },
          }
          assert_search(expected, request)
        end

        def test_static_value
          expected = {
            "sections-result" => {
              "records" => [
                {
                  "single_quote_string" => "string value",
                  "double_quote_string" => "string value",
                  "integer" => 29,
                  "complex_negative_number" => -29.29,
                },
                {
                  "single_quote_string" => "string value",
                  "double_quote_string" => "string value",
                  "integer" => 29,
                  "complex_negative_number" => -29.29,
                },
              ],
            },
          }
          request = {
            "queries" => {
              "sections-result" => {
                "source" => "Sections",
                "output" => {
                  "elements" => [
                    "records",
                  ],
                  "format" => "complex",
                  "limit" => 2,
                  "attributes" => [
                    {
                      "label" => "single_quote_string",
                      "source" => "'string value'",
                    },
                    {
                      "label" => "double_quote_string",
                      "source" => '"string value"',
                    },
                    {
                      "label" => "integer",
                      "source" => "29",
                    },
                    {
                      "label" => "complex_negative_number",
                      "source" => "-29.29",
                    },
                  ],
                },
              },
            },
          }
          assert_search(expected, request)
        end

        def test_expression
          expected = {
            "sections-result" => {
              "records" => [
                {
                  "formatted title" => "<Groonga overview>",
                  "title" => "Groonga overview",
                },
              ],
            },
          }
          request = {
            "queries" => {
              "sections-result" => {
                "source" => "Sections",
                "output" => {
                  "elements" => [
                    "records",
                  ],
                  "format" => "complex",
                  "limit" => 1,
                  "attributes" => [
                    "title",
                    {
                      "label" => "formatted title",
                      "source" => "'<' + title + '>'",
                    },
                  ],
                },
              },
            },
          }
          assert_search(expected, request)
        end

        def test_snippet_html
          expected = {
            "sections-result" => {
              "records" => [
                {
                  "title" => "Groonga overview",
                  "snippet" => [
                    "<span class=\"keyword\">Groonga</span> overview",
                  ],
                },
              ],
            },
          }
          request = {
            "queries" => {
              "sections-result" => {
                "source" => "Sections",
                "condition" => {
                  "query" => "Groonga",
                  "matchTo" => ["title"],
                },
                "output" => {
                  "elements" => [
                    "records",
                  ],
                  "format" => "complex",
                  "limit" => 1,
                  "attributes" => [
                    "title",
                    {
                      "label" => "snippet",
                      "source" => "snippet_html(title)",
                    },
                  ],
                },
              },
            },
          }
          assert_search(expected, request)
        end
      end

      class FormatTest < self
        def test_complex
          request = {
            "queries" => {
              "sections-result" => {
                "source" => "Sections",
                "output" => {
                  "elements" => [
                    "records",
                  ],
                  "format" => "complex",
                  "limit" => 3,
                  "attributes" => ["_key", "title"],
                },
              },
            },
          }
          assert_search(complex_result, request)
        end

        def test_simple
          request = {
            "queries" => {
              "sections-result" => {
                "source" => "Sections",
                "output" => {
                  "elements" => [
                    "records",
                  ],
                  "format" => "simple",
                  "limit" => 3,
                  "attributes" => ["_key", "title"],
                },
              },
            },
          }
          assert_search(simple_result, request)
        end

        def test_default
          request = {
            "queries" => {
              "sections-result" => {
                "source" => "Sections",
                "output" => {
                  "elements" => [
                    "records",
                  ],
                  "limit" => 3,
                  "attributes" => ["_key", "title"],
                },
              },
            },
          }
          assert_search(simple_result, request)
        end

        def complex_result
          {
            "sections-result" => {
              "records" => [
                {
                  "_key" => "1.1",
                  "title" => "Groonga overview",
                },
                {
                  "_key" => "1.2",
                  "title" => "Full text search and Instant update",
                },
                {
                  "_key" => "1.3",
                  "title" => "Column store and aggregate query",
                },
              ],
            },
          }
        end

        def simple_result
          {
            "sections-result" => {
              "records" => [
                ["1.1", "Groonga overview"],
                ["1.2", "Full text search and Instant update"],
                ["1.3", "Column store and aggregate query"],
              ],
            },
          }
        end
      end
    end
  end

  class ReferenceTest < self
    class Hash
      def setup_data
        restore(fixture_data("reference/hash.grn"))
      end

      def test_reference_to_hash
        expected = {
          "sections-result" => {
            "records" => [
              {
                "_key" => "1.1",
                "document" => "Groonga",
              },
              {
                "_key" => "1.2",
                "document" => "Groonga",
              },
              {
                "_key" => "1.3",
                "document" => "Groonga",
              },
            ],
          },
        }
        request = {
          "queries" => {
            "sections-result" => {
              "source" => "SectionsForHash",
              "output" => {
                "elements" => [
                  "records",
                ],
                "format" => "complex",
                "limit" => 3,
                "attributes" => ["_key", "document"],
              },
            },
          },
        }
        assert_search(expected, request)
      end
    end

    class Array
      def setup_data
        restore(fixture_data("reference/array.grn"))
      end

      def test_reference_to_array
        expected = {
          "sections-result" => {
            "records" => [
              {
                "_key" => "1.1",
                "document" => 1,
              },
              {
                "_key" => "1.2",
                "document" => 1,
              },
              {
                "_key" => "1.3",
                "document" => 1,
              },
            ],
          },
        }
        request = {
          "queries" => {
            "sections-result" => {
              "source" => "SectionsForArray",
              "output" => {
                "elements" => [
                  "records",
                ],
                "format" => "complex",
                "limit" => 3,
                "attributes" => ["_key", "document"],
              },
            },
          },
        }
        assert_search(expected, request)
      end
    end
  end
end
