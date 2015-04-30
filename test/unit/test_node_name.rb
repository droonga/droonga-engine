# Copyright (C) 2014-2015 Droonga Project
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

require "droonga/node_name"

class NodeNameTest < Test::Unit::TestCase
  def node_name(host, port, tag)
    Droonga::NodeName.new(:host => host,
                          :port => port,
                          :tag  => tag)
  end

  class ParseTest < self
    def parse(string)
      Droonga::NodeName.parse(string)
    end

    def test_valid_string
      assert_equal(node_name("192.168.0.1", 2929, "droonga"),
                   parse("192.168.0.1:2929/droonga"))
    end

    def test_instance
      assert_equal(node_name("192.168.0.1", 2929, "droonga"),
                   parse(node_name("192.168.0.1", 2929, "droonga")))
    end

    data(:no_host => ":2929/droonga",
         :no_port => "192.168.0.1/droonga",
         :no_tag  => "192.168.0.1:2929",
         :blank   => "",
         :nil     => nil)
    def test_invalid(input)
      assert_raise(ArgumentError) do
        parse(input)
      end
    end
  end

  class FormatterTest < self
    def test_node
      assert_equal("192.168.0.1:2929/droonga",
                   node_name("192.168.0.1", 2929, "droonga").node)
    end

    def test_string
      assert_equal("192.168.0.1:2929/droonga",
                   node_name("192.168.0.1", 2929, "droonga").to_s)
    end
  end
end
