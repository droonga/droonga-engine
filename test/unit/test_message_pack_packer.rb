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
  def test_to_msgpack
    src = [
      11,
      29,
      Time.parse("2013-11-29T08:00:00Z"),
      "Groonga",
      {"key" => "value"}
    ]
    actual = Droonga::MessagePackPacker.to_msgpack(src)
    expected = "\x95\v\x1D\xB42013-11-29T08:00:00Z\xA7Groonga\x81\xA3key\xA5value".force_encoding("ASCII-8BIT")

    assert_equal(expected, actual)
  end
end
