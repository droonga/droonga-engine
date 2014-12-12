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

require "droonga/catalog/dataset"
require "droonga/catalog/slices_volume"

class CatalogSingleVolumeTest < Test::Unit::TestCase
  def create_slices(data)
    minimum_dataset_data = {
      "replicas" => {
      },
    }
    dataset = Droonga::Catalog::Dataset.new("DatasetName", minimum_dataset_data)
    Droonga::Catalog::SlicesVolume.new(dataset, data)
  end

  class DimensionTest < self
    def test_default
      data = {
        "slices" => [],
      }
      volume = create_slices(data)
      assert_equal("_key", volume.dimension)
    end

    def test_specified
      data = {
        "dimension" => "group",
        "slices" => [],
      }
      volume = create_slices(data)
      assert_equal("group", volume.dimension)
    end
  end

  class SlicerTest < self
    def test_default
      data = {
        "slices" => [],
      }
      volume = create_slices(data)
      assert_equal("hash", volume.slicer)
    end

    def test_specified
      data = {
        "slicer" => "ordinal",
      }
      volume = create_slices(data)
      assert_equal("ordinal", volume.slicer)
    end
  end

  class SlicesTest < self
    def test_empty
      data = {
        "slices" => [],
      }
      volume = create_slices(data)
      assert_equal([], volume.slices)
    end
  end

  class RatioOrderSlicerTest < self
    class TotalWeightTest < self
      def test_three_slices
        data = {
          "slicer" => "hash",
          "slices" => [
            {
              "weight" => 10,
            },
            {
              "weight" => 20,
            },
            {
              "weight" => 30,
            },
          ],
        }
        assert_equal(10 + 20 + 30,
                     total_weight(data))
      end

      private
      def total_weight(data)
        volume = create_slices(data)
        volume.send(:compute_total_weight)
      end
    end
  end

  class AllNodesTest < self
    def test_slices
      data = {
        "slices" => [
          { "volume" => { "address" => "127.0.0.1:23003/droonga.000" } },
          { "volume" => { "address" => "127.0.0.1:23003/droonga.001" } },
          { "volume" => { "address" => "127.0.0.1:23004/droonga.100" } },
          { "volume" => { "address" => "127.0.0.1:23004/droonga.101" } },
        ],
      }
      volume = create_slices(data)
      assert_equal(["127.0.0.1:23003/droonga", "127.0.0.1:23004/droonga"],
                   volume.all_nodes)
    end

    def test_replicas_in_slice
      data = {
        "slices" => [
          {
            "volume" => {
              "replicas" => [
                { "address" => "127.0.0.1:23003/droonga.000" },
                { "address" => "127.0.0.1:23003/droonga.001" },
              ],
            },
          },
          {
            "volume" => {
              "replicas" => [
                { "address" => "127.0.0.1:23004/droonga.100" },
                { "address" => "127.0.0.1:23004/droonga.101" },
              ],
            },
          },
        ],
      }
      volume = create_slices(data)
      assert_equal(["127.0.0.1:23003/droonga", "127.0.0.1:23004/droonga"],
                   volume.all_nodes)
    end

    def test_slices_in_replicas_in_slice
      data = {
        "slices" => [
          {
            "volume" => {
              "replicas" => [
                {
                  "slices" => [
                    { "volume" => { "address" => "127.0.0.1:23003/droonga.000" } },
                    { "volume" => { "address" => "127.0.0.1:23003/droonga.001" } },
                  ],
                },
              ],
            },
          },
          {
            "volume" => {
              "replicas" => [
                {
                  "slices" => [
                    { "volume" => { "address" => "127.0.0.1:23004/droonga.100" } },
                    { "volume" => { "address" => "127.0.0.1:23004/droonga.101" } },
                  ],
                },
              ],
            },
          },
        ],
      }
      volume = create_slices(data)
      assert_equal(["127.0.0.1:23003/droonga", "127.0.0.1:23004/droonga"],
                   volume.all_nodes)
    end
  end
end
