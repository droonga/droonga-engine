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

require "droonga/catalog/volume_collection"

class CatalogVolumeCollectionTest < Test::Unit::TestCase
  private
  def create_volume_collection(volumes)
    Droonga::Catalog::VolumeCollection.new(volumes)
  end

  class SelectTest < self
    def setup
      volumes = [
        "volume1",
        "volume2",
        "volume3",
      ]
      @collection = create_volume_collection(volumes)
    end

    def test_top
      assert_equal(["volume1"], @collection.select(:top))
    end

    def test_random
      random_volumes = @collection.select(:random).collect do |volume|
        volume.gsub(/\Avolume[123]\z/, "any volume")
      end
      assert_equal(["any volume"], random_volumes)
    end

    def test_all
      assert_equal(["volume1", "volume2", "volume3"],
                   @collection.select(:all))
    end
  end
end
