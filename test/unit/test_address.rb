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

require "droonga/address"

class AddressTest < Test::Unit::TestCase
  def address(host, port, tag, local_name)
    Droonga::Address.new(:host => host,
                         :port => port,
                         :tag  => tag,
                         :local_name => local_name)
  end

  class ParseTest < self
    def parse(string)
      Droonga::Address.parse(string)
    end

    data(:full => {
           :input      => "node29:2929/droonga.local_name",
           :host       => "node29",
           :port       => 2929,
           :tag        => "droonga",
           :local_name => "local_name",
         },
         :ip_address => {
           :input      => "192.168.0.1:2929/droonga.local_name",
           :host       => "192.168.0.1",
           :port       => 2929,
           :tag        => "droonga",
           :local_name => "local_name",
         },
         :internal_name => {
           :input      => "node29:2929/droonga.\#1",
           :host       => "node29",
           :port       => 2929,
           :tag        => "droonga",
           :local_name => "#1",
         },
         :no_local_name => {
           :input      => "node29:2929/droonga",
           :host       => "node29",
           :port       => 2929,
           :tag        => "droonga",
           :local_name => nil,
         })
    def test_success(data)
      assert_equal(address(data[:host], data[:port], data[:tag],
                           data[:local_name]),
                   parse(data[:input]))
    end

    data(:no_host => ":2929/droonga",
         :no_port => "192.168.0.1/droonga",
         :no_tag  => "192.168.0.1:2929",
         :blank   => "",
         :nil     => nil)
    def test_fail(input)
      assert_raise(ArgumentError) do
        parse(input)
      end
    end
  end

  class AttributeTest < self
    def test_attributes
      address = address("192.168.0.1", 2929, "droonga", "000")
      assert_equal({:host       => "192.168.0.1",
                    :port       => 2929,
                    :tag        => "droonga",
                    :local_name => "000"},
                   {:host       => address.host,
                    :port       => address.port,
                    :tag        => address.tag,
                    :local_name => address.local_name})
    end
  end

  class ComparisonTest < self
    def test_address_vs_string
      string  = "192.168.0.1:2929/droonga.000"
      address = address("192.168.0.1", 2929, "droonga", "000")
      assert_true(address == string)
    end

    #XXX This is a confusable behavior. It seems should be true
    #    but actually false, so you have to be careful when you
    #    compare string with Address.
    def test_string_vs_address
      string  = "192.168.0.1:2929/droonga.000"
      address = address("192.168.0.1", 2929, "droonga", "000")
      assert_false(string == address)
    end
  end

  class FormatterTest < self
    def test_node
      assert_equal("192.168.0.1:2929/droonga",
                   address("192.168.0.1", 2929, "droonga", "000").node)
    end

    data(:full => {
           :expected   => "192.168.0.1:2929/droonga.000",
           :host       => "192.168.0.1",
           :port       => 2929,
           :tag        => "droonga",
           :local_name => "000",
         },
         :no_local_name => {
           :expected   => "192.168.0.1:2929/droonga",
           :host       => "192.168.0.1",
           :port       => 2929,
           :tag        => "droonga",
           :local_name => nil,
         })
    def test_string(data)
      assert_equal(data[:expected],
                   address(data[:host], data[:port], data[:tag],
                           data[:local_name]).to_s)
    end

    data(:full => {
           :expected   => ["192.168.0.1", 2929, "droonga", "000"],
           :host       => "192.168.0.1",
           :port       => 2929,
           :tag        => "droonga",
           :local_name => "000",
         },
         :no_local_name => {
           :expected   => ["192.168.0.1", 2929, "droonga", nil],
           :host       => "192.168.0.1",
           :port       => 2929,
           :tag        => "droonga",
           :local_name => nil,
         })
    def test_array(data)
      assert_equal(data[:expected],
                   address(data[:host], data[:port], data[:tag],
                           data[:local_name]).to_a)
    end
  end
end
