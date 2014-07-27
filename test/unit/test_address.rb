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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "droonga/address"

class AddressTest < Test::Unit::TestCase
  def address(host, port, tag, name)
    Droonga::Address.new(:host => host,
                         :port => port,
                         :tag  => tag,
                         :name => name)
  end

  class ParseTest < self
    def parse(string)
      Droonga::Address.parse(string)
    end

    def test_full
      assert_equal(address("192.168.0.1", 2929, "droonga", "name"),
                   parse("192.168.0.1:2929/droonga.name"))
    end

    def test_internal_name
      assert_equal(address("192.168.0.1", 2929, "droonga", "#1"),
                   parse("192.168.0.1:2929/droonga.\#1"))
    end

    def test_no_name
      assert_equal(address("192.168.0.1", 2929, "droonga", nil),
                   parse("192.168.0.1:2929/droonga"))
    end
  end

  class FormatterTest < self
    def test_node
      assert_equal("192.168.0.1:2929/droonga",
                   address("192.168.0.1", 2929, "droonga", "000").node)
    end
  end
end
