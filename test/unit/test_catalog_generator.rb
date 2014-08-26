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

require "droonga/catalog_generator"

class CatalogGeneratorTest < Test::Unit::TestCase
  def setup
    @generator = Droonga::CatalogGenerator.new
    @normalized_time_value = "2014-02-09T00:00:00Z"
  end

  def generate
    normalize_catalog(@generator.generate)
  end

  def normalize_catalog(catalog)
    normalize_time(catalog, "effectiveDate")
    catalog
  end

  def normalize_time(catalog, key)
    begin
      Time.iso8601(catalog[key])
    rescue ArgumentError
      # Do nothing for invalid time value
    else
      catalog[key] = @normalized_time_value
    end
  end

  def default_plugins
    ["groonga", "search", "crud", "dump", "system", "catalog"]
  end

  def test_default
    catalog = {
      "version" => 2,
      "effectiveDate" => @normalized_time_value,
      "datasets" => {
      },
    }
    assert_equal(catalog, generate)
  end

  class DatasetTest < self
    def test_default
      @generator.add_dataset("Droonga", {})
      dataset = {
        "nWorkers" => 4,
        "plugins" => default_plugins,
        "schema" => {},
        "replicas" => [
          {
            "dimension" => "_key",
            "slicer" => "hash",
            "slices" => [
              {
                "volume" => {
                  "address" => "127.0.0.1:10031/droonga.000",
                },
                "weight" => 100,
              },
            ],
          },
        ],
      }
      assert_equal(dataset, generate["datasets"]["Droonga"])
    end

    def test_n_workers
      @generator.add_dataset("Droonga", :n_workers => 3)
      assert_equal(3, generate["datasets"]["Droonga"]["nWorkers"])
    end

    def test_plugins
      @generator.add_dataset("Droonga", :plugins => ["search"])
      assert_equal(["search"], generate["datasets"]["Droonga"]["plugins"])
    end

    def test_fact
      @generator.add_dataset("Droonga", :fact => "Entries")
      assert_equal("Entries", generate["datasets"]["Droonga"]["fact"])
    end

    class LoadTest < self
      def test_replicas
        dataset = {
          "nWorkers" => 4,
          "plugins" => default_plugins,
          "schema" => [],
          "replicas" => [
            {
              "dimension" => "_key",
              "slicer" => "hash",
              "slices" => [
                {
                  "volume" => {
                    "address" => "127.0.0.1:10031/droonga.000",
                  },
                  "weight" => 100,
                },
              ],
            },
          ],
        }
        catalog = {
          "datasets" => {
            "Default" => dataset,
          }
        }
        @generator.load(catalog)
        assert_equal(dataset, generate["datasets"]["Default"])
      end
    end
  end

  class ReplicasTest < self
    def test_catalog
      replicas = [
        {
          "dimension" => "_key",
          "slicer" => "hash",
          "slices" => [
            {
              "volume" => {
                "address" => "127.0.0.1:10031/droonga.000",
              },
              "weight" => 100,
            },
          ],
        },
      ]
      @generator.add_dataset("Droonga", :replicas => replicas)
      dataset = {
        "nWorkers" => 4,
        "plugins" => default_plugins,
        "schema" => {},
        "replicas" => replicas,
      }
      assert_equal(dataset, generate["datasets"]["Droonga"])
    end
  end
end
