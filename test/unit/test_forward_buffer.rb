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

require "time"

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

    @forwarded_messages = []
    @buffer.on_forward = lambda do |message, destination|
      @forwarded_messages << {:message => message,
                              :destination => destination}
    end
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

  def test_add
    @buffer.add({}, {})
    @buffer.add({}, {})
    @buffer.add({}, {})
    assert_equal(3, @buffer.buffered_messages.size)
  end

  class ForwardTest < self
    def setup
      super
      @buffer.add({"date" => "2015-04-30T00:00:00.000000Z"}, {})
      @buffer.add({"date" => "2015-04-30T01:00:00.000000Z"}, {})
      @buffer.add({"date" => "2015-04-30T02:00:00.000000Z"}, {})
    end

    def test_without_boundary
      @buffer.start_forward
      assert_equal({
                     :empty => true,
                     :forwarded => [
                       {:message => {"date" => "2015-04-30T00:00:00.000000Z",
                                     "xSender"=>"forward-buffer"},
                        :destination => {}},
                       {:message => {"date" => "2015-04-30T01:00:00.000000Z",
                                     "xSender"=>"forward-buffer"},
                        :destination => {}},
                       {:message => {"date" => "2015-04-30T02:00:00.000000Z",
                                     "xSender"=>"forward-buffer"},
                        :destination => {}},
                     ],
                   },
                   {
                     :empty => @buffer.empty?,
                     :forwarded => @forwarded_messages,
                   })
    end

    def test_with_boundary
      @buffer.process_messages_newer_than(Time.parse("2015-04-30T01:00:00.000000Z"))
      @buffer.start_forward
      assert_equal({
                     :empty => true,
                     :forwarded => [
                       {:message => {"date" => "2015-04-30T02:00:00.000000Z",
                                     "xSender"=>"forward-buffer"},
                        :destination => {}},
                     ],
                   },
                   {
                     :empty => @buffer.empty?,
                     :forwarded => @forwarded_messages,
                   })
    end
  end
end
