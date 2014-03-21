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

require "droonga/catalog/collection_volume"

class CatalogSingleVolumeTest < Test::Unit::TestCase
  def create_collection_volume(data)
    Droonga::Catalog::CollectionVolume.new(data)
  end

  class DimensionTest < self
    def test_default
      data = {
      }
      volume = create_collection_volume(data)
      assert_equal("_key", volume.dimension)
    end

    def test_specified
      data = {
        "dimension" => "group",
      }
      volume = create_collection_volume(data)
      assert_equal("group", volume.dimension)
    end
  end

  class SlicerTest < self
    def test_default
      data = {
      }
      volume = create_collection_volume(data)
      assert_equal("hash", volume.slicer)
    end

    def test_specified
      data = {
        "slicer" => "ordinal",
      }
      volume = create_collection_volume(data)
      assert_equal("ordinal", volume.slicer)
    end
  end

  class SlicesTest < self
    def test_empty
      data = {
        "slices" => [],
      }
      volume = create_collection_volume(data)
      assert_equal([], volume.slices)
    end
  end
end
