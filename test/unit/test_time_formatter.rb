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

require "droonga/time_formatter"

class TimeFormatterTest < Test::Unit::TestCase
  def test_fraction
    w3c_dtf_time = "2013-11-29T08:00:00.292929Z"
    time = Time.parse(w3c_dtf_time)
    assert_equal(w3c_dtf_time, format(time))
  end

  private
  def format(time)
    Droonga::TimeFormatter.format(time)
  end
end
