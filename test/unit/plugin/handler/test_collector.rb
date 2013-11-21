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
              "my_number_key" => 1,
              "my_string_key" => "a",
              "my_array_key" => [1, 2, 3],
            },
          },
          "component" => {
            "body" => {
              input_name => {
                output_name => {
                  "my_number_key" => ["sum"],
                  "my_string_key" => ["sum"],
                  "my_array_key" => ["sum"],
                },
              },
            },
            "outputs" => nil,
          },
        },
        "id" => nil,
        "value" => {
          "my_number_key" => 2,
          "my_string_key" => "b",
          "my_array_key" => [4, 5, 6],
        },
        "name" => input_name,
        "descendants" => nil,
      }
      @handler.handle("collector_reduce", request)
      assert_equal([
                     {
                       "my_number_key" => 3,
                       "my_string_key" => "ab",
                       "my_array_key" => [1, 2, 3, 4, 5, 6],
                     },
                     output_name
                   ],
                   @messages.last)
    end
  end
end
