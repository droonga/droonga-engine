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

require "droonga/catalog/dataset"

class CatalogDatasetTest < Test::Unit::TestCase
  private
  def create_dataset(data)
    Droonga::Catalog::Dataset.new("DatasetName", data)
  end

  class NameTest < self
    def test_reader
      dataset = Droonga::Catalog::Dataset.new("DatasetName", {})
      assert_equal("DatasetName", dataset.name)
    end
  end

  class NWorkersTest < self
    def test_default
      data = {
      }
      dataset = create_dataset(data)
      assert_equal(0, dataset.n_workers)
    end

    def test_specified
      data = {
        "nWorkers" => 2
      }
      dataset = create_dataset(data)
      assert_equal(2, dataset.n_workers)
    end
  end

  class SchemaTest < self
    def test_empty
      data = {
        "schema" => {
        }
      }
      dataset = create_dataset(data)
      assert_equal(Droonga::Catalog::Schema.new("dataset_name", {}),
                   dataset.schema)
    end
  end

  class PluginsTest < self
    def test_default
      data = {
      }
      dataset = create_dataset(data)
      assert_equal([], dataset.plugins)
    end

    def test_multiple
      data = {
        "plugins" => ["groonga", "crud"],
      }
      dataset = create_dataset(data)
      assert_equal(["groonga", "crud"], dataset.plugins)
    end
  end

  class FactTest < self
    def test_default
      data = {
      }
      dataset = create_dataset(data)
      assert_nil(dataset.fact)
    end

    def test_specified
      data = {
        "fact" => "Users",
      }
      dataset = create_dataset(data)
      assert_equal("Users", dataset.fact)
    end
  end

  class ReplicasTest < self
    def test_empty
      data = {
        "replicas" => [],
      }
      dataset = create_dataset(data)
      assert_equal(Droonga::Catalog::VolumeCollection.new([]),
                   dataset.replicas)
    end
  end

  class NodesTest < self
    def test_all_nodes
      data = {
        "replicas" => [
          {
            "dimension" =>"_key",
            "slicer" =>"hash",
            "slices" =>[
              { "volume" =>{ "address" =>"127.0.0.1:23003/droonga.000" } },
              { "volume" =>{ "address" =>"127.0.0.1:23004/droonga.001" } },
              { "volume" =>{ "address" =>"127.0.0.1:23003/droonga.002" } },
              { "volume" =>{ "address" =>"127.0.0.1:23005/droonga.003" } },
            ],
          },
          {
            "dimension" =>"_key",
            "slicer" =>"hash",
            "slices" =>[
              { "volume" =>{ "address" =>"127.0.0.1:23004/droonga.000" } },
              { "volume" =>{ "address" =>"127.0.0.1:23005/droonga.001" } },
              { "volume" =>{ "address" =>"127.0.0.1:23003/droonga.002" } },
              { "volume" =>{ "address" =>"127.0.0.1:23004/droonga.003" } },
            ],
          },
        ],
      }
      dataset = create_dataset(data)
      assert_equal([
                     "127.0.0.1:23003/droonga",
                     "127.0.0.1:23004/droonga",
                     "127.0.0.1:23005/droonga",
                   ],
                   dataset.all_nodes)
    end
  end
end
