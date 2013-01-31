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
          "count" => 9,
        }
      }
      assert_equal(expected,
                   @worker.process_message(request))
    end
  end
end
