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

require "droonga/message_pack_packer"

class MessagePackPackerTest < Test::Unit::TestCase
  def test_integer
    assert_equal(29, unpack(pack(29)))
  end

  def test_string
    assert_equal("Droonga", unpack(pack("Droonga")))
  end

  def test_time
    w3c_dtf_time = "2013-11-29T08:00:00Z"
    time = Time.parse(w3c_dtf_time)
    assert_equal(w3c_dtf_time, unpack(pack(time)))
  end

  def test_hash
    hash = {"key" => "value"}
    assert_equal(hash, unpack(pack(hash)))
  end

  def test_array
    array = ["Groonga", "Rroonga", "Droonga"]
    assert_equal(array, unpack(pack(array)))
  end

  private
  def pack(object)
    Droonga::MessagePackPacker.pack(object)
  end

  def unpack(msgpack)
    MessagePack.unpack(msgpack)
  end
end
