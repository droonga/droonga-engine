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

require "droonga/reducer"

class ReducerTest < Test::Unit::TestCase
  private
  def create_record(*columns)
    columns
  end

  def reduce_value(deal, left_value, right_value)
    reducer = Droonga::Reducer.new(deal)
    reducer.reduce(left_value, right_value)
  end

  class << self
    def create_record(*columns)
      columns
    end
  end

  data(
    :int => {
      :expected => 3,
      :left     => 1,
      :right    => 2,
    },
    :float => {
      :expected => 3.0,
      :left     => 1.0,
      :right    => 2.0,
    },
    :string => {
      :expected => "ab",
      :left     => "a",
      :right    => "b",
    },
    :array => {
      :expected => [0, 1],
      :left     => [0],
      :right    => [1],
    },
    :hash => {
      :expected => {:a => 0, :b => 1, :c => 2},
      :left     => {:a => 0, :c => 2},
      :right    => {:b => 1, :c => 3},
    },
    :nested_hash => {
      :expected => {:a => 0, :b => 1, :c => {:d => 2}},
      :left     => {:a => 0, :c => {:d => 2}},
      :right    => {:b => 1, :c => {:e => 3}},
    },
    :nil_left => {
      :expected => 0,
      :left     => nil,
      :right    => 0,
    },
    :nil_right => {
      :expected => 0,
      :left     => 0,
      :right    => nil,
    },
    :nil_both => {
      :expected => nil,
      :left     => nil,
      :right    => nil,
    },
  )
  def test_sum(data)
    reduced = reduce_value({ "type" => "sum" },
                           data[:left],
                           data[:right])
    assert_equal(data[:expected], reduced)
  end

  data(
    :int => {
      :expected => 3,
      :left     => 1,
      :right    => 2,
    },
    :float => {
      :expected => 3.0,
      :left     => 1.0,
      :right    => 2.0,
    },
    :string => {
      :expected => "ab",
      :left     => "a",
      :right    => "b",
    },
    :array => {
      :expected => [3],
      :left     => [1],
      :right    => [2],
    },
    :hash => {
      :expected => {:a => 0, :b => 1, :c => 5},
      :left     => {:a => 0, :c => 2},
      :right    => {:b => 1, :c => 3},
    },
    :nested_hash => {
      :expected => {:a => 0, :b => 1, :c => {:d => 2, :e => 3}},
      :left     => {:a => 0, :c => {:d => 2}},
      :right    => {:b => 1, :c => {:e => 3}},
    },
    :nil_left => {
      :expected => 0,
      :left     => nil,
      :right    => 0,
    },
    :nil_right => {
      :expected => 0,
      :left     => 0,
      :right    => nil,
    },
    :nil_both => {
      :expected => nil,
      :left     => nil,
      :right    => nil,
    },
  )
  def test_recursive_sum(data)
    reduced = reduce_value({ "type" => "recursive-sum" },
                           data[:left],
                           data[:right])
    assert_equal(data[:expected], reduced)
  end

  data(
    :int => {
      :expected => 1.5,
      :left     => 1,
      :right    => 2,
    },
    :float => {
      :expected => 1.5,
      :left     => 1.0,
      :right    => 2.0,
    },
  )
  def test_average(data)
    reduced = reduce_value({ "type" => "average" },
                           data[:left],
                           data[:right])
    assert_equal(data[:expected], reduced)
  end

  data(
    :true_and_false => {
      :expected => false,
      :left     => true,
      :right    => false,
    },
    :false_and_true => {
      :expected => false,
      :left     => false,
      :right    => true,
    },
    :both_true => {
      :expected => true,
      :left     => true,
      :right    => true,
    },
    :both_false => {
      :expected => false,
      :left     => false,
      :right    => false,
    },
  )
  def test_and(data)
    reduced = reduce_value({ "type" => "and" },
                           data[:left],
                           data[:right])
    assert_equal(data[:expected], reduced)
  end

  data(
    :true_and_false => {
      :expected => true,
      :left     => true,
      :right    => false,
    },
    :false_and_true => {
      :expected => true,
      :left     => false,
      :right    => true,
    },
    :both_true => {
      :expected => true,
      :left     => true,
      :right    => true,
    },
    :both_false => {
      :expected => false,
      :left     => false,
      :right    => false,
    },
  )
  def test_or(data)
    reduced = reduce_value({ "type" => "or" },
                           data[:left],
                           data[:right])
    assert_equal(data[:expected], reduced)
  end

  class ReduceRecords < self
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
      reduced = reduce_value({ "type" => "sum",
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
      reduced = reduce_value({
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

  class MergeRecords < self
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

      reduced = reduce_value({
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
