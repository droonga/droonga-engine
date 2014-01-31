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

  class IOTest < self
    data(
      :simple_mapping => {
        :expected => ["output_name", "result"],
        :source => "result",
        :mapping => "output_name",
      },
      :complex_mapping => {
        :expected => ["output_name", "result"],
        :source => "result",
        :mapping => {
          "output" => "output_name",
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
      assert_equal(data[:expected], @outputs.last)
    end

    def test_reduce
      input_name = "input_#{Time.now.to_i}"
      output_name = "output_#{Time.now.to_i}"
      request = {
        "task" => {
          "values" => {
            output_name => [0, 1, 2],
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
        "value" => [3, 4, 5],
        "name" => input_name,
        "descendants" => nil,
      }
      @plugin.process("collector_reduce", request)
      assert_equal([
                     output_name,
                     [0, 1, 2, 3, 4, 5],
                   ],
                   @outputs.last)
    end
  end

  class ReduceTest < self
    data(
      :int => {
        :expected => 1.5,
        :left => 1,
        :right => 2,
      },
      :float => {
        :expected => 1.5,
        :left => 1.0,
        :right => 2.0,
      },
    )
    def test_average(data)
      reduced = @plugin.reduce({ "type" => "average" },
                               data[:left],
                               data[:right])
      assert_equal(data[:expected], reduced)
    end

    data(
      :true_and_false => {
        :expected => false,
        :left => true,
        :right => false,
      },
      :false_and_true => {
        :expected => false,
        :left => false,
        :right => true,
      },
      :both_true => {
        :expected => true,
        :left => true,
        :right => true,
      },
      :both_false => {
        :expected => false,
        :left => false,
        :right => false,
      },
    )
    def test_and(data)
      reduced = @plugin.reduce({ "type" => "and" },
                               data[:left],
                               data[:right])
      assert_equal(data[:expected], reduced)
    end

    data(
      :true_and_false => {
        :expected => true,
        :left => true,
        :right => false,
      },
      :false_and_true => {
        :expected => true,
        :left => false,
        :right => true,
      },
      :both_true => {
        :expected => true,
        :left => true,
        :right => true,
      },
      :both_false => {
        :expected => false,
        :left => false,
        :right => false,
      },
    )
    def test_or(data)
      reduced = @plugin.reduce({ "type" => "or" },
                               data[:left],
                               data[:right])
      assert_equal(data[:expected], reduced)
    end

    data(
      :numeric_values => {
        :expected => 3,
        :left => 1,
        :right => 2,
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
        :left => [
          create_record(1),
          create_record(2),
          create_record(3),
        ],
        :right => [
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
        :left => [
          create_record("a"),
          create_record("b"),
          create_record("c"),
        ],
        :right => [
          create_record("d"),
          create_record("e"),
          create_record("f"),
        ],
      },
      :numeric_values_with_limit => {
        :expected => 3,
        :left => 1,
        :right => 2,
        :limit => 2,
      },
      :numeric_key_records_with_limit => {
        :expected => [
          create_record(1),
          create_record(2),
        ],
        :left => [
          create_record(1),
          create_record(2),
          create_record(3),
        ],
        :right => [
          create_record(4),
          create_record(5),
          create_record(6),
        ],
        :limit => 2,
      },
      :string_key_records_with_limit => {
        :expected => [
          create_record("a"),
          create_record("b"),
        ],
        :left => [
          create_record("a"),
          create_record("b"),
          create_record("c"),
        ],
        :right => [
          create_record("d"),
          create_record("e"),
          create_record("f"),
        ],
        :limit => 2,
      },
    )
    def test_sum(data)
      reduced = @plugin.reduce({ "type" => "sum",
                                 "limit" => data[:limit] || -1 },
                               data[:left],
                               data[:right])
      assert_equal(data[:expected], reduced)
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
        :left => [
          create_record(1),
          create_record(3),
          create_record(5),
        ],
        :right => [
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
        :left => [
          create_record("a"),
          create_record("c"),
          create_record("e"),
        ],
        :right => [
          create_record("b"),
          create_record("d"),
          create_record("f"),
        ],
      },
      :numeric_key_records_with_limit => {
        :expected => [
          create_record(1),
          create_record(2),
        ],
        :left => [
          create_record(1),
          create_record(3),
          create_record(5),
        ],
        :right => [
          create_record(2),
          create_record(4),
          create_record(6),
        ],
        :limit => 2,
      },
      :string_key_records_with_limit => {
        :expected => [
          create_record("a"),
          create_record("b"),
        ],
        :left => [
          create_record("a"),
          create_record("c"),
          create_record("e"),
        ],
        :right => [
          create_record("b"),
          create_record("d"),
          create_record("f"),
        ],
        :limit => 2,
      },
    )
    def test_sort(data)
      reduced = @plugin.reduce({ 
                                 "type" => "sort",
                                 "operators" => [
                                   { "column" => 0, "operator" => "<" },
                                 ],
                                 "limit" => data[:limit] || -1,
                               },
                               data[:left],
                               data[:right])
      assert_equal(data[:expected], reduced)
    end
  end

  class MergeTest < self
    def test_grouped
      expected = [
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
      ]

      left = [
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
      ]
      right = [
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
      ]

      reduced = @plugin.reduce({ 
                                 "type" => "sort",
                                 "operators" => [
                                   { "column" => 1, "operator" => "<" },
                                 ],
                                 "key_column" => 0,
                                 "limit" => -1,
                               },
                               left,
                               right)
      assert_equal(expected, reduced)
    end
  end
end
