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

require "droonga/plugin/collector/basic"

class BasicCollectorTest < Test::Unit::TestCase
  def setup
    setup_database
    @plugin = Droonga::BasicCollector.new
    @outputs = []
    stub(@plugin).emit do |name, value|
      @outputs << [name, value]
    end
  end

  def teardown
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
      assert_equal([output_name, data[:expected]], @outputs.last)
    end
  end

  class ReduceTest < self
    data(
      :numeric_values => {
        :expected => 3,
        :value => 1,
        :source => 2,
      },
      :numeric_key_records => {
        :expected => [
          create_record(1),
          create_record(2),
          create_record(3),
          create_record(4),
          create_record(5),
          create_record(6),
        ],
        :value => [
          create_record(1),
          create_record(2),
          create_record(3),
        ],
        :source => [
          create_record(4),
          create_record(5),
          create_record(6),
        ],
      },
      :string_key_records => {
        :expected => [
          create_record("a"),
          create_record("b"),
          create_record("c"),
          create_record("d"),
          create_record("e"),
          create_record("f"),
        ],
        :value => [
          create_record("a"),
          create_record("b"),
          create_record("c"),
        ],
        :source => [
          create_record("d"),
          create_record("e"),
          create_record("f"),
        ],
      },
    )
    def test_sum(data)
      input_name = "input_#{Time.now.to_i}"
      output_name = "output_#{Time.now.to_i}"
      request = {
        "task" => {
          "values" => {
            output_name => data[:value],
          },
          "component" => {
            "body" => {
              input_name => {
                output_name => {
                  "type" => "sum",
                  "limit" => -1,
                },
              },
            },
            "outputs" => nil,
          },
        },
        "id" => nil,
        "value" => data[:source],
        "name" => input_name,
        "descendants" => nil,
      }
      @plugin.process("collector_reduce", request)
      assert_equal([
                     output_name,
                     data[:expected],
                   ],
                   @outputs.last)
    end

    data(
      :numeric_values => {
        :expected => 3,
        :value => 1,
        :source => 2,
        :limit => 2,
      },
      :numeric_key_records => {
        :expected => [
          create_record(1),
          create_record(2),
        ],
        :value => [
          create_record(1),
          create_record(2),
          create_record(3),
        ],
        :source => [
          create_record(4),
          create_record(5),
          create_record(6),
        ],
        :limit => 2,
      },
      :string_key_records => {
        :expected => [
          create_record("a"),
          create_record("b"),
        ],
        :value => [
          create_record("a"),
          create_record("b"),
          create_record("c"),
        ],
        :source => [
          create_record("d"),
          create_record("e"),
          create_record("f"),
        ],
        :limit => 2,
      },
    )
    def test_sum_with_limit(data)
      input_name = "input_#{Time.now.to_i}"
      output_name = "output_#{Time.now.to_i}"
      request = {
        "task" => {
          "values" => {
            output_name => data[:value],
          },
          "component" => {
            "body" => {
              input_name => {
                output_name => {
                  "type" => "sum",
                  "limit" => data[:limit],
                },
              },
            },
            "outputs" => nil,
          },
        },
        "id" => nil,
        "value" => data[:source],
        "name" => input_name,
        "descendants" => nil,
      }
      @plugin.process("collector_reduce", request)
      assert_equal([
                     output_name,
                     data[:expected],
                   ],
                   @outputs.last)
    end

    data(
      :numeric_key_records => {
        :expected => [
          create_record(1),
          create_record(2),
          create_record(3),
          create_record(4),
          create_record(5),
          create_record(6),
        ],
        :value => [
          create_record(1),
          create_record(3),
          create_record(5),
        ],
        :source => [
          create_record(2),
          create_record(4),
          create_record(6),
        ],
      },
      :string_key_records => {
        :expected => [
          create_record("a"),
          create_record("b"),
          create_record("c"),
          create_record("d"),
          create_record("e"),
          create_record("f"),
        ],
        :value => [
          create_record("a"),
          create_record("c"),
          create_record("e"),
        ],
        :source => [
          create_record("b"),
          create_record("d"),
          create_record("f"),
        ],
      },
    )
    def test_sort(data)
      input_name = "input_#{Time.now.to_i}"
      output_name = "output_#{Time.now.to_i}"
      request = {
        "task" => {
          "values" => {
            output_name => data[:value],
          },
          "component" => {
            "body" => {
              input_name => {
                output_name => {
                  "type" => "sort",
                  "operators" => [
                    { "column" => 0, "operator" => "<" },
                  ],
                  "limit" => -1,
                },
              },
            },
            "outputs" => nil,
          },
        },
        "id" => nil,
        "value" => data[:source],
        "name" => input_name,
        "descendants" => nil,
      }
      @plugin.process("collector_reduce", request)
      assert_equal([
                     output_name,
                     data[:expected],
                   ],
                   @outputs.last)
    end


    data(
      :numeric_key_records => {
        :expected => [
          create_record(1),
          create_record(2),
        ],
        :value => [
          create_record(1),
          create_record(3),
          create_record(5),
        ],
        :source => [
          create_record(2),
          create_record(4),
          create_record(6),
        ],
        :limit => 2,
      },
      :string_key_records => {
        :expected => [
          create_record("a"),
          create_record("b"),
        ],
        :value => [
          create_record("a"),
          create_record("c"),
          create_record("e"),
        ],
        :source => [
          create_record("b"),
          create_record("d"),
          create_record("f"),
        ],
        :limit => 2,
      },
    )
    def test_sort_with_limit(data)
      input_name = "input_#{Time.now.to_i}"
      output_name = "output_#{Time.now.to_i}"
      request = {
        "task" => {
          "values" => {
            output_name => data[:value],
          },
          "component" => {
            "body" => {
              input_name => {
                output_name => {
                  "type" => "sort",
                  "operators" => [
                    { "column" => 0, "operator" => "<" },
                  ],
                  "limit" => 2,
                },
              },
            },
            "outputs" => nil,
          },
        },
        "id" => nil,
        "value" => data[:source],
        "name" => input_name,
        "descendants" => nil,
      }
      @plugin.process("collector_reduce", request)
      assert_equal([
                     output_name,
                     data[:expected],
                   ],
                   @outputs.last)
    end
  end

  class MergeTest < self
    def test_grouped
      input_name = "input_#{Time.now.to_i}"
      output_name = "output_#{Time.now.to_i}"
      request = {
        "task" => {
          "values" => {
            output_name => [
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
          "component" => {
            "body" => {
              input_name => {
                output_name => {
                    "type" => "sort",
                    "operators" => [
                      { "column" => 1, "operator" => "<" },
                    ],
                    "key_column" => 0,
                    "limit" => -1,
                },
              },
            },
            "outputs" => nil,
          },
        },
        "id" => nil,
        "value" => [
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
        "name" => input_name,
        "descendants" => nil,
      }
      @plugin.process("collector_reduce", request)
      assert_equal([
                     output_name,
                     [
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
                   ],
                   @outputs.last)
    end
  end
end
