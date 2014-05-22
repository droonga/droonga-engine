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
  def create_single_volume(data)
    Droonga::Catalog::SingleVolume.new(data)
  end

  def test_address
    data = {
      "address" => "127.0.0.1:10047/volume.000",
    }
    volume = create_single_volume(data)
    assert_equal("127.0.0.1:10047/volume.000",
                 volume.address)
  end

  def test_all_nodes
    data = {
      "address" => "127.0.0.1:10047/volume.000",
    }
    volume = create_single_volume(data)
    assert_equal(["127.0.0.1:10047/volume"],
                 volume.all_nodes)
  end
end
