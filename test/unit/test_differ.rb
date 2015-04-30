# Copyright (C) 2015 Droonga Project
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

require "droonga/differ"

class DifferTest < Test::Unit::TestCase
  data(:string => {
         :left     => "a",
         :right    => "b",
         :expected => "\"a\" <=> \"b\"",
       },
       :numeric => {
         :left     => 0,
         :right    => 1,
         :expected => "0 <=> 1",
       },
       :hash => {
         :left     => {:a => 0, :b => 1},
         :right    => {:a => 0, :b => 2},
         :expected => {:b => "1 <=> 2"},
       },
       :array => {
         :left     => [0, 1, 2],
         :right    => [0, 1, 3],
         :expected => {2 => "2 <=> 3"},
       },
       :nested => {
         :left     => {:a => 0, :b => {:aa => 0, :bb => 1}},
         :right    => {:a => 1, :b => {:aa => 0, :bb => 2}},
         :expected => {:a => "0 <=> 1",
                       :b => {:bb => "1 <=> 2"}},
       })
  def test_diff(data)
    assert_equal(data[:expected],
                 Droonga::Differ.diff(data[:left], data[:right]))
  end
end
