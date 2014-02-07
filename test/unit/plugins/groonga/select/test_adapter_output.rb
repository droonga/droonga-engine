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

require "droonga/plugins/groonga/select"

class GroongaSelectAdapterOutputTest < Test::Unit::TestCase
  private
  def convert(search_response)
    converter = Droonga::Plugins::Groonga::Select::ResponseConverter.new
    converter.convert(search_response)
  end

  class RecordsTest < self
    def test_empty
      start_time = "2001-08-02T10:45:23.5+09:00"
      elapsed_time = 0
      count = 0

      search_response = {
        "EmptyTable" => {
          "startTime"   => start_time,
          "elapsedTime" => elapsed_time,
          "count"       => count,
          "attributes"  => [
            {"name" => "_id", "type" => "UInt32", "vector" => false},
          ],
          "records"     => [],
        },
      }

      status_code = 0
      start_time_in_unix_time = Time.parse(start_time).to_f
      headers = [["_id","UInt32"]]
      expected_select_response = [
        [status_code, start_time_in_unix_time, elapsed_time],
        [[[count], headers]],
      ]

      assert_equal(expected_select_response, convert(search_response))
    end
  end
end
