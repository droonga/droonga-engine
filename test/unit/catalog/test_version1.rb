# Copyright (C) 2013-2014 Droonga Project
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

require "droonga/catalog/version1"

class CatalogTest < Test::Unit::TestCase
  class << self
    def minimum_data
      {
        "effective_date" => "2013-09-01T00:00:00Z",
        "zones" => [],
        "farms" => {},
        "datasets" => {},
      }
    end
  end

  private
  def minimum_data
    self.class.minimum_data
  end

  def create_catalog(data, path)
    Droonga::Catalog::Version1.new(data, path)
  end

  class OptionTest < self
    def create_catalog(options)
      super(minimum_data.merge("options" => options), "path")
    end

    def test_nonexistent
      catalog = create_catalog({})
      assert_nil(catalog.option("nonexistent"))
    end

    def test_existent
      catalog = create_catalog("plugins" => ["crud", "groonga"])
      assert_equal(["crud", "groonga"],
                   catalog.option("plugins"))
    end
  end

  class PartitionTest < self
    def setup
      data = JSON.parse(File.read(catalog_path))
      @catalog = create_catalog(data, catalog_path)
    end

    def test_get_partitions
      partitions = @catalog.get_partitions("localhost:23003/test")
      assert_equal({
                     "localhost:23003/test.000" => {
                       :database  => "#{base_path}/000/db",
                       :plugins   => ["for_dataset"],
                       :n_workers => 0
                     },
                     "localhost:23003/test.001" => {
                       :database  => "#{base_path}/001/db",
                       :plugins   => ["for_dataset"],
                       :n_workers => 0
                     },
                     "localhost:23003/test.002" => {
                       :database  => "#{base_path}/002/db",
                       :plugins   => ["for_dataset"],
                       :n_workers => 0
                     },
                     "localhost:23003/test.003" => {
                       :database  => "#{base_path}/003/db",
                       :plugins   => ["for_dataset"],
                       :n_workers => 0
                     },
                   },
                   partitions)
    end

    def fixture_path(base_path)
      File.expand_path("../../fixtures/#{base_path}", __FILE__)
    end

    def catalog_path
      @catalog_path ||= fixture_path("catalog/version1.json")
    end

    def base_path
      File.dirname(catalog_path)
    end

    class PluginsTest < self
      def setup
        @data = minimum_data.merge({
          "zones" => [farm_name],
          "farms" => {
            farm_name => {
              "device" => ".",
            },
          },
          "datasets" => {
            "Droonga" => {
              "workers" => 1,
              "number_of_partitions" => 1,
              "number_of_replicas" => 1,
              "date_range" => "infinity",
              "partition_key" => "_key",
              "plugins" => [],
              "ring" => {
                "localhost:23041" => {
                  "weight" =>  50,
                  "partitions" => {
                    "2014-02-09" => [
                      "#{farm_name}.000",
                    ],
                  },
                },
              },
            },
          },
        })
      end

      def farm_name
        "localhost:23041/droonga"
      end

      def plugins(data)
        catalog = create_catalog(data, catalog_path)
        catalog.get_partitions(farm_name).collect do |partition, options|
          options[:plugins]
        end
      end

      def test_plugins
        @data["datasets"]["Droonga"]["plugins"] = ["search", "groonga", "add"]
        assert_equal([["search", "groonga", "add"]],
                     plugins(@data))

      end
    end
  end

  class DataSetTest < self
    class RingTest < self
      class TotalWeightTest < self
        def test_three_zones
          dataset = {
            "ring" => {
              "zone1" => {
                "weight" => 10,
              },
              "zone2" => {
                "weight" => 20,
              },
              "zone3" => {
                "weight" => 30,
              },
            }
          }
          assert_equal(10 + 20 + 30,
                       total_weight(dataset))
        end

        private
        def total_weight(dataset)
          catalog = create_catalog(minimum_data,
                                   "base-path")
          catalog.send(:compute_total_weight, dataset)
        end
      end
    end
  end

  class ValidationTest < self
    class << self
      def farm_name
        "localhost:23041/droonga"
      end

      def ring_name
        "localhost:23041"
      end

      def range_name
        "2013-09-01"
      end

      def path
        "path/to/catalog"
      end

      def valid_farms
        {
          farm_name => {
            "device" => ".",
          },
        }
      end

      def valid_zones
        valid_farms.keys
      end

      def valid_dataset_base
        {
          "workers" => 0,
          "number_of_replicas" => 1,
          "number_of_partitions" => 1,
          "partition_key" => "_key",
          "date_range" => "infinity",
          "ring" => {},
        }
      end
    end

    data(
      :missing_root_elements => {
        :catalog => {},
        :errors => [
          Droonga::Catalog::MissingRequiredParameter.new(
            "effective_date", path),
          Droonga::Catalog::MissingRequiredParameter.new(
            "zones", path),
          Droonga::Catalog::MissingRequiredParameter.new(
            "farms", path),
          Droonga::Catalog::MissingRequiredParameter.new(
            "datasets", path),
        ],
      },
      :invalid_farms => {
        :catalog => minimum_data.merge(
          "farms" => {
            farm_name => {
            },
          },
          "zones" => [farm_name],
        ),
        :errors => [
          Droonga::Catalog::MissingRequiredParameter.new(
            "farms.#{farm_name}.device", path),
        ],
      },
      :missing_dataset_parameters => {
        :catalog => minimum_data.merge(
          "farms" => valid_farms,
          "zones" => valid_zones,
          "datasets" => {
            "Droonga" => {
            },
          },
        ),
        :errors => [
          Droonga::Catalog::MissingRequiredParameter.new(
            "datasets.Droonga.workers", path),
          Droonga::Catalog::MissingRequiredParameter.new(
            "datasets.Droonga.number_of_replicas", path),
          Droonga::Catalog::MissingRequiredParameter.new(
            "datasets.Droonga.number_of_partitions", path),
          Droonga::Catalog::MissingRequiredParameter.new(
            "datasets.Droonga.partition_key", path),
          Droonga::Catalog::MissingRequiredParameter.new(
            "datasets.Droonga.date_range", path),
          Droonga::Catalog::MissingRequiredParameter.new(
            "datasets.Droonga.ring", path),
        ],
      },
      :invalid_date_parameters => {
        :catalog => minimum_data.merge(
          "effective_date" => "invalid",
        ),
        :errors => [
          Droonga::Catalog::InvalidDate.new(
            "effective_date", "invalid", path),
        ],
      },
      :non_integer_numeric_parameters => {
        :catalog => minimum_data.merge(
          "farms" => valid_farms,
          "zones" => valid_zones,
          "datasets" => {
            "Droonga" => valid_dataset_base.merge(
              "workers" => 0.1,
              "number_of_replicas" => 0.1,
              "number_of_partitions" => 0.1,
              "ring" => {
                ring_name => {
                  "weight" => 0.1,
                  "partitions" => {},
                },
              },
            ),
          },
        ),
        :errors => [
          Droonga::Catalog::MismatchedParameterType.new(
            "datasets.Droonga.workers", Integer, Float, path),
          Droonga::Catalog::MismatchedParameterType.new(
            "datasets.Droonga.number_of_replicas", Integer, Float, path),
          Droonga::Catalog::MismatchedParameterType.new(
            "datasets.Droonga.number_of_partitions", Integer, Float, path),
        ],
      },
      :negative_numeric_parameters => {
        :catalog => minimum_data.merge(
          "farms" => valid_farms,
          "zones" => valid_zones,
          "datasets" => {
            "Droonga" => valid_dataset_base.merge(
              "workers" => -1,
              "number_of_replicas" => -1,
              "number_of_partitions" => -1,
              "ring" => {
                ring_name => {
                  "weight" => -1,
                  "partitions" => {},
                },
              },
            ),
          },
        ),
        :errors => [
          Droonga::Catalog::NegativeNumber.new(
            "datasets.Droonga.workers", -1, path),
          Droonga::Catalog::SmallerThanOne.new(
            "datasets.Droonga.number_of_replicas", -1, path),
          Droonga::Catalog::SmallerThanOne.new(
            "datasets.Droonga.number_of_partitions", -1, path),
          Droonga::Catalog::NegativeNumber.new(
            "datasets.Droonga.ring.#{ring_name}.weight", -1, path),
        ],
      },
      :broken_relations_unknown_farm => {
        :catalog => minimum_data.merge(
          "farms" => valid_farms,
          "zones" => valid_zones,
          "datasets" => {
            "Droonga" => valid_dataset_base.merge(
              "ring" => {
                ring_name => {
                  "weight" => 1,
                  "partitions" => {
                    range_name => [
                      "unknown:0/unknown.000",
                    ],
                  },
                },
              },
            ),
          },
        ),
        :errors => [
          Droonga::Catalog::UnknownFarmForPartition.new(
            "datasets.Droonga.ring.#{ring_name}.partitions.#{range_name}[0]",
            "unknown:0/unknown.000", path),
        ],
      },
    )
    def test_validation(data)
      begin
        create_catalog(data[:catalog], "path/to/catalog")
        assert_nil("must not reach here")
      rescue Droonga::MultiplexError => actual_errors
        actual_errors = actual_errors.errors.collect do |error|
          error.message
        end.sort
        expected_errors = data[:errors].collect do |error|
          error.message
        end.sort
        assert_equal(expected_errors, actual_errors)
      end
    end
  end
end
