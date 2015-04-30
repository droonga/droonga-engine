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

require "droonga/forward_buffer"

class ForwardBufferTest < Test::Unit::TestCase
  class ForwardBuffer < Droonga::ForwardBuffer
    attr_writer :data_directory, :serf
  end

  def setup
    setup_temporary_directory
    @buffer = ForwardBuffer.new("node29:2929/droonga")
    @buffer.data_directory = @temporary_directory
    @buffer.serf = StubSerf.new
  end

  def teardown
    teardown_temporary_directory
  end

  def test_empty
    assert_true(@buffer.empty?)
  end

  def test_not_empty
    @buffer.add({}, {})
    assert_false(@buffer.empty?)
  end
end
