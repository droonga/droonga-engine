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

  class ValidationTest < self
    data(:ip_address => "192.168.0.1:2929/droonga",
         :host_name  => "node29:2929/droonga")
    def test_valid(input)
      assert_true(Droonga::NodeName.valid?(input))
    end

    data(:no_host => ":2929/droonga",
         :no_port => "192.168.0.1/droonga",
         :no_tag  => "192.168.0.1:2929",
         :blank   => "",
         :nil     => nil)
    def test_invalid(input)
      assert_false(Droonga::NodeName.valid?(input))
    end
  end

  class DefaultParameterTest < self
    data(:omitted => {
           :params => {
           },
           :host  => Droonga::NodeName::DEFAULT_HOST,
           :port  => Droonga::NodeName::DEFAULT_PORT,
           :tag   => Droonga::NodeName::DEFAULT_TAG,
         },
         :nil => {
           :params => {
             :host => nil,
             :port => nil,
             :tag  => nil,
           },
           :host  => Droonga::NodeName::DEFAULT_HOST,
           :port  => Droonga::NodeName::DEFAULT_PORT,
           :tag   => Droonga::NodeName::DEFAULT_TAG,
         },
         :filled => {
           :params => {
             :host => "node29",
             :port => 2929,
             :tag  => "test",
           },
           :host  => "node29",
           :port  => 2929,
           :tag   => "test",
         })
    def test_default_parameter(data)
      assert_equal(node_name(data[:host], data[:port], data[:tag]),
                   Droonga::NodeName.new(data[:params]))
    end
  end

  class ParseTest < self
    def parse(string)
      Droonga::NodeName.parse(string)
    end

    data(:ip_address => {
           :input => "192.168.0.1:2929/droonga",
           :host  => "192.168.0.1",
           :port  => 2929,
           :tag   => "droonga",
         },
         :host_name => {
           :input => "node29:2929/droonga",
           :host  => "node29",
           :port  => 2929,
           :tag   => "droonga",
         })
    def test_valid_string(data)
      assert_equal(node_name(data[:host], data[:port], data[:tag]),
                   parse(data[:input]))
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

  class AttributeTest < self
    def test_attributes
      node_name = node_name("192.168.0.1", 2929, "droonga")
      assert_equal({:host => "192.168.0.1",
                    :port => 2929,
                    :tag  => "droonga"},
                   {:host => node_name.host,
                    :port => node_name.port,
                    :tag  => node_name.tag})
    end
  end

  class ComparisonTest < self
    def test_node_name_vs_string
      string    = "192.168.0.1:2929/droonga"
      node_name = node_name("192.168.0.1", 2929, "droonga")
      assert_true(node_name == string)
    end

    #XXX This is a confusable behavior. It seems should be true
    #    but actually false, so you have to be careful when you
    #    compare string with Nodename.
    def test_string_vs_node_name
      string    = "192.168.0.1:2929/droonga"
      node_name = node_name("192.168.0.1", 2929, "droonga")
      assert_false(string == node_name)
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

    def test_array
      assert_equal(["192.168.0.1", 2929, "droonga"],
                   node_name("192.168.0.1", 2929, "droonga").to_a)
    end
  end
end
