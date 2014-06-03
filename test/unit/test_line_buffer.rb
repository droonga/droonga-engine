# Copyright (C) 2014 Droonga Project
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

require "droonga/line_buffer"

class LineBufferTest < Test::Unit::TestCase
  def setup
    @line_buffer = Droonga::LineBuffer.new
  end

  def feed(data)
    lines = []
    @line_buffer.feed(data) do |line|
      lines << line
    end
    lines
  end

  class NoBufferTest < self
    def test_no_new_line
      assert_equal([], feed("a"))
    end

    def test_one_new_line
      assert_equal(["a\n"], feed("a\n"))
    end

    def test_multiple_new_lines
      assert_equal(["a\n", "b\n"], feed("a\nb\n"))
    end
  end

  class BufferedTest < self
    def test_one_line
      assert_equal([], feed("a"))
      assert_equal(["a\n"], feed("\n"))
    end

    def test_multiple_lines
      assert_equal([], feed("a"))
      assert_equal(["a\n", "b\n"], feed("\nb\n"))
    end

    def test_multiple_buffered
      assert_equal([], feed("a"))
      assert_equal(["a\n"], feed("\nb"))
      assert_equal(["b\n"], feed("\n"))
    end
  end
end
