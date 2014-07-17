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

require "droonga/catalog/single_volume"

class CatalogSingleVolumeTest < Test::Unit::TestCase
  class AddressTest < self
    def setup
      data = {
        "address" => "127.0.0.1:10047/tag.000",
      }
      @volume = Droonga::Catalog::SingleVolume.new(data)
    end

    def test_address
      assert_equal("127.0.0.1:10047/tag.000",
                   @volume.address)
    end

    def test_host
      assert_equal("127.0.0.1", @volume.host)
    end

    def test_port
      assert_equal(10047, @volume.port)
    end

    def test_tag
      assert_equal("tag", @volume.tag)
    end

    def test_name
      assert_equal("000", @volume.name)
    end

    def test_node
      assert_equal("127.0.0.1:10047/tag", @volume.node)
    end

    def test_all_nodes
      assert_equal(["127.0.0.1:10047/tag"],
                   @volume.all_nodes)
    end
  end
end
