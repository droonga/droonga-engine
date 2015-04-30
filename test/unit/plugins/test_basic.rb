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

require "droonga/plugins/basic"

class BasicCollectorTest < Test::Unit::TestCase
  def setup
    setup_database
  end

  def teardown
    teardown_database
  end

  def run_collector(collector, message)
    collector_message = Droonga::CollectorMessage.new(message)
    collector.collect(collector_message)
    collector_message.values
  end

  def gather(message)
    collector = Droonga::Plugins::Basic::GatherCollector.new
    run_collector(collector, message)
  end

  def reduce(message)
    collector = Droonga::Plugins::Basic::ReduceCollector.new
    run_collector(collector, message)
  end

  class IOTest < self
    data(
      :simple_mapping => {
        :expected => { "output_name" => "result" },
        :source => "result",
        :mapping => "output_name",
      },
      :complex_mapping => {
        :expected => { "output_name" => "result" },
        :source => "result",
        :mapping => {
          "output" => "output_name",
        },
      },
    )
    def test_gather(data)
      request = {
        "task" => {
          "values" => {},
          "step" => {
            "body" => nil,
            "outputs" => nil,
          },
        },
        "id" => nil,
        "value" => data[:source],
        "name" => data[:mapping],
        "descendants" => nil,
      }
      assert_equal(data[:expected], gather(request))
    end

    def test_reduce
      input_name = "input_#{Time.now.to_i}"
      output_name = "output_#{Time.now.to_i}"
      request = {
        "task" => {
          "values" => {
            output_name => [0, 1, 2],
          },
          "step" => {
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
      assert_equal({ output_name => [0, 1, 2, 3, 4, 5] },
                   reduce(request))
    end
  end
end
