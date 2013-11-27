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

  class GatherTest < self
    def test_gather
      input_name = "input_#{Time.now.to_i}"
      input_value = "value_#{Time.now.to_i}"
      request = {
        "task" => {
          "values" => nil,
          "component" => {
            "body" => nil,
            "outputs" => nil,
          },
        },
        "id" => nil,
        "value" => input_value,
        "name" => input_name,
        "descendants" => nil,
      }
      @plugin.process("collector_gather", request)
      assert_equal([input_value, input_name], @messages.last)
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
                  "numeric_value" => { "type" => "sum" },
                  "numeric_key_records" => { "type" => "sum" },
                  "string_key_records" => { "type" => "sum" },
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

    def test_sum_with_offset_and_limit
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
                    "offset" => 2,
                    "limit" => 2,
                  },
                  "numeric_key_records" => {
                    "type" => "sum",
                    "offset" => 2,
                    "limit" => 2,
                  },
                  "string_key_records" => {
                    "type" => "sum",
                    "offset" => 3,
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
                         create_record(3),
                         create_record(4),
                       ],
                       "string_key_records" => [
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
                    "order" => ["<"],
                  },
                  "string_key_records" => {
                    "type" => "sort",
                    "order" => ["<"],
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

    def test_sort_with_limit_and_offset
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
                    "order" => ["<"],
                    "offset" => 2,
                    "limit" => 2,
                  },
                  "string_key_records" => {
                    "type" => "sort",
                    "order" => ["<"],
                    "offset" => 3,
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
                         create_record(3),
                         create_record(4),
                       ],
                       "string_key_records" => [
                         create_record("d"),
                         create_record("e"),
                         create_record("f"),
                       ],
                     },
                     output_name
                   ],
                   @messages.last)
    end

    private
    def create_record(*columns)
      columns
    end
  end
end
