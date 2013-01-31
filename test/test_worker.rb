# Copyright (C) 2013 Kotoumi project
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

require "helper"

require "kotoumi/worker"

class WorkerTest < Test::Unit::TestCase
  def setup
    setup_database
    setup_worker
  end

  def teardown
    teardown_worker
  end

  private
  def setup_database
    restore(fixture_data("document.grn"))
  end

  def setup_worker
    @worker = Kotoumi::Worker.new(@database_path.to_s, "KotoumiQueue")
  end

  def teardown_worker
    @worker.shutdown
    @worker = nil
  end

  private
  class SearchTest < self
    def test_minimum
      request = {
        "type" => "search",
        "body" => {
          "queries" => {
            "sections" => {
              "source" => "Sections",
            },
          }
        },
      }
      expected = {
        "sections" => {
          "startTime" => start_time,
          "elapsedTime" => elapsed_time,
          "count" => 9,
        }
      }
      actual = @worker.process_message(request)
      assert_equal(expected, normalize_result_set(actual))
    end

    private
    def start_time
      "2013-01-31T14:34:47+09:00"
    end

    def elapsed_time
      0.01
    end

    def normalize_result_set(result_set)
      normalized_result_set = copy_deeply(result_set)
      normalized_result_set.each do |name, result|
        result["startTime"] = start_time if result["startTime"]
        result["elapsedTime"] = elapsed_time if result["elapsedTime"]
      end
      normalized_result_set
    end

    def copy_deeply(object)
      Marshal.load(Marshal.dump(object))
    end
  end
end
