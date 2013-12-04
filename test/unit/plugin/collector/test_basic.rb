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

require "droonga/plugin/collector/basic"

class BasicCollectorTest < Test::Unit::TestCase
  include PluginHelper

  def setup
    setup_database
    setup_plugin(Droonga::BasicCollector)
  end

  def teardown
    teardown_plugin
    teardown_database
  end

  private
  def create_record(*columns)
    columns
  end

  class << self
    def create_record(*columns)
      columns
    end
  end

  class GatherTest < self
    data(
      :simple_mapping => {
        :expected => "result",
        :source => "result",
        :mapping => "string_name",
      },
      :complex_mapping => {
        :expected => {
          "count" => 3,
          "records" => [
            create_record(0),
            create_record(1),
            create_record(2),
          ],
        },
        :source => {
          "count" => 3,
          "records" => [
            create_record(0),
            create_record(1),
            create_record(2),
          ],
        },
        :mapping => {
          "output" => "search_result",
        },
      },
      :offset_and_limit => {
        :expected => {
          "count" => 3,
          "records" => [
            create_record(1),
          ],
        },
        :source => {
          "count" => 3,
          "records" => [
            create_record(0),
            create_record(1),
            create_record(2),
          ],
        },
        :mapping => {
          "output" => "search_result",
          "element" => "records",
          "offset" => 1,
          "limit" => 1,
        },
      },
      :offset_and_unlimited_limit => {
        :expected => {
          "count" => 3,
          "records" => [
            create_record(1),
            create_record(2),
          ],
        },
        :source => {
          "count" => 3,
          "records" => [
            create_record(0),
            create_record(1),
            create_record(2),
          ],
        },
        :mapping => {
          "output" => "search_result",
          "element" => "records",
          "offset" => 1,
          "limit" => -1,
        },
      },
      :attributes => {
        :expected => {
          "count" => 2,
          "records" => [
            create_record(1, 1.1, 1.2, 1.3, 1.4),
            create_record(2, 2.1, 2.2, 2.3, 2.4),
          ],
        },
        :source => {
          "count" => 2,
          "records" => [
            create_record(1, 1.1, 1.2, 1.3, 1.4),
            create_record(2, 2.1, 2.2, 2.3, 2.4),
          ],
        },
        :mapping => {
          "output" => "search_result",
          "element" => "records",
          "attributes" => ["_key", "chapter", "section", "subsection", "paragraph"],
          "limit" => -1,
        },
      },
      :attributes_with_sort_attributes => {
        :expected => {
          "count" => 2,
          "records" => [
            create_record(1, 1.1, 1.2),
            create_record(2, 2.1, 2.2),
          ],
        },
        :source => {
          "count" => 2,
          "records" => [
            create_record(1, 1.1, 1.2, 1.3, 1.4),
            create_record(2, 2.1, 2.2, 2.3, 2.4),
          ],
        },
        :mapping => {
          "output" => "search_result",
          "element" => "records",
          "attributes" => ["_key", "chapter", "section"],
          "limit" => -1,
        },
      },
      :format_simple => {
        :expected => {
          "count" => 2,
          "records" => [
            create_record(1, 1.1, 1.2),
            create_record(2, 2.1, 2.2),
          ],
        },
        :source => {
          "count" => 2,
          "records" => [
            create_record(1, 1.1, 1.2),
            create_record(2, 2.1, 2.2),
          ],
        },
        :mapping => {
          "output" => "search_result",
          "element" => "records",
          "format" => "simple",
          "attributes" => ["_key", "chapter", "section"],
          "limit" => -1,
        },
      },
      :format_complex => {
        :expected => {
          "count" => 2,
          "records" => [
            { "_key" => 1, "chapter" => 1.1, "section" => 1.2 },
            { "_key" => 2, "chapter" => 2.1, "section" => 2.2 },
          ],
        },
        :source => {
          "count" => 2,
          "records" => [
            create_record(1, 1.1, 1.2),
            create_record(2, 2.1, 2.2),
          ],
        },
        :mapping => {
          "output" => "search_result",
          "element" => "records",
          "format" => "complex",
          "attributes" => ["_key", "chapter", "section"],
          "limit" => -1,
        },
      },
    )
    def test_gather(data)
      request = {
        "task" => {
          "values" => nil,
          "component" => {
            "body" => nil,
            "outputs" => nil,
          },
        },
        "id" => nil,
        "value" => data[:source],
        "name" => data[:mapping],
        "descendants" => nil,
      }
      @plugin.process("collector_gather", request)
      output_name = data[:mapping]
      output_name = output_name["output"] if output_name.is_a?(Hash)
      assert_equal([data[:expected], output_name], @messages.last)
    end
  end

  class ReduceTest < self
    def test_sum
      input_name = "input_#{Time.now.to_i}"
      output_name = "output_#{Time.now.to_i}"
      request = {
        "task" => {
          "values" => {
            output_name => {
              "numeric_value" => 1,
              "numeric_key_records" => [
                create_record(1),
                create_record(2),
                create_record(3),
              ],
              "string_key_records" => [
                create_record("a"),
                create_record("b"),
                create_record("c"),
              ],
            },
          },
          "component" => {
            "body" => {
              input_name => {
                output_name => {
                  "numeric_value" => {
                    "type" => "sum",
                    "limit" => -1,
                  },
                  "numeric_key_records" => {
                    "type" => "sum",
                    "limit" => -1,
                  },
                  "string_key_records" => {
                    "type" => "sum",
                    "limit" => -1,
                  },
                },
              },
            },
            "outputs" => nil,
          },
        },
        "id" => nil,
        "value" => {
          "numeric_value" => 2,
          "numeric_key_records" => [
            create_record(4),
            create_record(5),
            create_record(6),
          ],
          "string_key_records" => [
            create_record("d"),
            create_record("e"),
            create_record("f"),
          ],
        },
        "name" => input_name,
        "descendants" => nil,
      }
      @plugin.process("collector_reduce", request)
      assert_equal([
                     {
                       "numeric_value" => 3,
                       "numeric_key_records" => [
                         create_record(1),
                         create_record(2),
                         create_record(3),
                         create_record(4),
                         create_record(5),
                         create_record(6),
                       ],
                       "string_key_records" => [
                         create_record("a"),
                         create_record("b"),
                         create_record("c"),
                         create_record("d"),
                         create_record("e"),
                         create_record("f"),
                       ],
                     },
                     output_name
                   ],
                   @messages.last)
    end

    def test_sum_with_limit
      input_name = "input_#{Time.now.to_i}"
      output_name = "output_#{Time.now.to_i}"
      request = {
        "task" => {
          "values" => {
            output_name => {
              "numeric_value" => 1,
              "numeric_key_records" => [
                create_record(1),
                create_record(2),
                create_record(3),
              ],
              "string_key_records" => [
                create_record("a"),
                create_record("b"),
                create_record("c"),
              ],
            },
          },
          "component" => {
            "body" => {
              input_name => {
                output_name => {
                  "numeric_value" => {
                    "type" => "sum",
                    "limit" => 2,
                  },
                  "numeric_key_records" => {
                    "type" => "sum",
                    "limit" => 2,
                  },
                  "string_key_records" => {
                    "type" => "sum",
                    "limit" => -1,
                  },
                },
              },
            },
            "outputs" => nil,
          },
        },
        "id" => nil,
        "value" => {
          "numeric_value" => 2,
          "numeric_key_records" => [
            create_record(4),
            create_record(5),
            create_record(6),
          ],
          "string_key_records" => [
            create_record("d"),
            create_record("e"),
            create_record("f"),
          ],
        },
        "name" => input_name,
        "descendants" => nil,
      }
      @plugin.process("collector_reduce", request)
      assert_equal([
                     {
                       "numeric_value" => 3,
                       "numeric_key_records" => [
                         create_record(1),
                         create_record(2),
                       ],
                       "string_key_records" => [
                         create_record("a"),
                         create_record("b"),
                         create_record("c"),
                         create_record("d"),
                         create_record("e"),
                         create_record("f"),
                       ],
                     },
                     output_name
                   ],
                   @messages.last)
    end

    def test_sort
      input_name = "input_#{Time.now.to_i}"
      output_name = "output_#{Time.now.to_i}"
      request = {
        "task" => {
          "values" => {
            output_name => {
              "numeric_key_records" => [
                create_record(1),
                create_record(3),
                create_record(5),
              ],
              "string_key_records" => [
                create_record("a"),
                create_record("c"),
                create_record("e"),
              ],
            },
          },
          "component" => {
            "body" => {
              input_name => {
                output_name => {
                  "numeric_key_records" => {
                    "type" => "sort",
                    "operators" => [
                      { "column" => 0, "operator" => "<" },
                    ],
                    "limit" => -1,
                  },
                  "string_key_records" => {
                    "type" => "sort",
                    "operators" => [
                      { "column" => 0, "operator" => "<" },
                    ],
                    "limit" => -1,
                  },
                },
              },
            },
            "outputs" => nil,
          },
        },
        "id" => nil,
        "value" => {
          "numeric_key_records" => [
            create_record(2),
            create_record(4),
            create_record(6),
          ],
          "string_key_records" => [
            create_record("b"),
            create_record("d"),
            create_record("f"),
          ],
        },
        "name" => input_name,
        "descendants" => nil,
      }
      @plugin.process("collector_reduce", request)
      assert_equal([
                     {
                       "numeric_key_records" => [
                         create_record(1),
                         create_record(2),
                         create_record(3),
                         create_record(4),
                         create_record(5),
                         create_record(6),
                       ],
                       "string_key_records" => [
                         create_record("a"),
                         create_record("b"),
                         create_record("c"),
                         create_record("d"),
                         create_record("e"),
                         create_record("f"),
                       ],
                     },
                     output_name
                   ],
                   @messages.last)
    end

    def test_sort_with_limit
      input_name = "input_#{Time.now.to_i}"
      output_name = "output_#{Time.now.to_i}"
      request = {
        "task" => {
          "values" => {
            output_name => {
              "numeric_key_records" => [
                create_record(1),
                create_record(3),
                create_record(5),
              ],
              "string_key_records" => [
                create_record("a"),
                create_record("c"),
                create_record("e"),
              ],
            },
          },
          "component" => {
            "body" => {
              input_name => {
                output_name => {
                  "numeric_key_records" => {
                    "type" => "sort",
                    "operators" => [
                      { "column" => 0, "operator" => "<" },
                    ],
                    "limit" => 2,
                  },
                  "string_key_records" => {
                    "type" => "sort",
                    "operators" => [
                      { "column" => 0, "operator" => "<" },
                    ],
                    "limit" => -1,
                  },
                },
              },
            },
            "outputs" => nil,
          },
        },
        "id" => nil,
        "value" => {
          "numeric_key_records" => [
            create_record(2),
            create_record(4),
            create_record(6),
          ],
          "string_key_records" => [
            create_record("b"),
            create_record("d"),
            create_record("f"),
          ],
        },
        "name" => input_name,
        "descendants" => nil,
      }
      @plugin.process("collector_reduce", request)
      assert_equal([
                     {
                       "numeric_key_records" => [
                         create_record(1),
                         create_record(2),
                       ],
                       "string_key_records" => [
                         create_record("a"),
                         create_record("b"),
                         create_record("c"),
                         create_record("d"),
                         create_record("e"),
                         create_record("f"),
                       ],
                     },
                     output_name
                   ],
                   @messages.last)
    end
  end

  class MergeTest < self
    def test_grouped
      input_name = "input_#{Time.now.to_i}"
      output_name = "output_#{Time.now.to_i}"
      request = {
        "task" => {
          "values" => {
            output_name => {
              "records" => [
                [
                  "group1",
                  10,
                  [
                    create_record(1),
                    create_record(3),
                    create_record(5),
                  ],
                ],
                [
                  "group2",
                  20,
                  [
                    create_record("a"),
                    create_record("c"),
                    create_record("e"),
                  ],
                ],
                [
                  "group3",
                  30,
                  [
                    create_record("A"),
                    create_record("B"),
                    create_record("C"),
                  ],
                ],
              ],
            },
          },
          "component" => {
            "body" => {
              input_name => {
                output_name => {
                  "records" => {
                    "type" => "sort",
                    "operators" => [
                      { "column" => 1, "operator" => "<" },
                    ],
                    "key_column" => 0,
                    "unified_columns" => [1, 2],
                    "limit" => -1,
                  },
                },
              },
            },
            "outputs" => nil,
          },
        },
        "id" => nil,
        "value" => {
          "records" => [
            [
              "group1",
              30,
              [
                create_record(2),
                create_record(4),
                create_record(6),
              ],
            ],
            [
              "group2",
              40,
              [
                create_record("b"),
                create_record("d"),
                create_record("f"),
              ],
            ],
            [
              "group4",
              50,
              [
                create_record("D"),
                create_record("E"),
                create_record("F"),
              ],
            ],
          ],
        },
        "name" => input_name,
        "descendants" => nil,
      }
      @plugin.process("collector_reduce", request)
      assert_equal([
                     {
                       "records" => [
                         [
                           "group3",
                           30,
                           [
                             create_record("A"),
                             create_record("B"),
                             create_record("C"),
                           ],
                         ],
                         [
                           "group1",
                           40,
                           [
                             create_record(2),
                             create_record(4),
                             create_record(6),
                             create_record(1),
                             create_record(3),
                             create_record(5),
                           ],
                         ],
                         [
                           "group4",
                           50,
                           [
                             create_record("D"),
                             create_record("E"),
                             create_record("F"),
                           ],
                         ],
                         [
                           "group2",
                           60,
                           [
                             create_record("b"),
                             create_record("d"),
                             create_record("f"),
                             create_record("a"),
                             create_record("c"),
                             create_record("e"),
                           ],
                         ],
                       ],
                     },
                     output_name
                   ],
                   @messages.last)
    end
  end
end
