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

require "droonga/plugin/handler/collector"

class BasicCollectorHandlerTest < Test::Unit::TestCase
  include HandlerHelper

  def setup
    setup_database
    setup_handler(Droonga::BasicCollectorHandler)
  end

  def teardown
    teardown_handler
    teardown_database
  end

  private

  public
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
      @handler.handle("collector_gather", request)
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
                  "numeric_key_records" => ["sum"],
                  "string_key_records" => ["sum"],
                },
              },
            },
            "outputs" => nil,
          },
        },
        "id" => nil,
        "value" => {
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
      @handler.handle("collector_reduce", request)
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
                  "numeric_key_records" => ["sort", "<"],
                  "string_key_records" => ["sort", "<"],
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
      @handler.handle("collector_reduce", request)
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

    private
    def create_record(key)
      [key]
    end
  end
end
