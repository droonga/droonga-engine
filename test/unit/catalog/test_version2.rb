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

require "droonga/catalog/version2"

class CatalogVersion2Test < Test::Unit::TestCase
  class << self
    def minimum_data
      {
        "effectiveDate" => "2014-02-28T00:00:00Z",
        "datasets" => {
        },
      }
    end
  end

  private
  def minimum_data
    self.class.minimum_data
  end

  def create_catalog(data, path)
    Droonga::Catalog::Version2.new(data, path)
  end

  class SliceTest < self
    def setup
      data = JSON.parse(File.read(catalog_path))
      @catalog = create_catalog(data, catalog_path)
    end

    def test_slices
      slices = @catalog.slices("localhost:23003/test")
      assert_equal({
                     "localhost:23003/test.000" => {
                       :database  => "#{base_path}/000/db",
                       :dataset   => "Test",
                       :plugins   => ["plugin1", "plugin2", "plugin3"],
                       :n_workers => 4,
                     },
                     "localhost:23003/test.001" => {
                       :database  => "#{base_path}/001/db",
                       :dataset   => "Test",
                       :plugins   => ["plugin1", "plugin2", "plugin3"],
                       :n_workers => 4,
                     },
                     "localhost:23003/test.002" => {
                       :database  => "#{base_path}/002/db",
                       :dataset   => "Test",
                       :plugins   => ["plugin1", "plugin2", "plugin3"],
                       :n_workers => 4,
                     },
                   },
                   slices)
    end

    def fixture_path(base_path)
      File.expand_path("../../fixtures/#{base_path}", __FILE__)
    end

    def catalog_path
      @catalog_path ||= fixture_path("catalog/version2.json")
    end

    def base_path
      File.dirname(catalog_path)
    end

    class PluginsTest < self
      def setup
        custom_data = {
          "datasets" => {
            "Droonga" => {
              "nWorkers" => 1,
              "replicas" => [
                {
                  "slices" => [
                    {
                      "volume" => {
                        "address" => "#{farm_name}.000",
                      },
                    },
                  ],
                },
              ],
            },
          },

        }
        @data = minimum_data.merge(custom_data)
      end

      def farm_name
        "localhost:23041/droonga"
      end

      def plugins(data)
        catalog = create_catalog(data, catalog_path)
        catalog.slices(farm_name).collect do |volum_address, options|
          options[:plugins]
        end
      end

      def test_plugins
        @data["datasets"]["Droonga"]["plugins"] = ["search", "groonga", "add"]
        assert_equal([["search", "groonga", "add"]],
                     plugins(@data))

      end
    end

    class NodesTest < self
      def test_all_nodes
        assert_equal(["localhost:23003/test", "localhost:23004/test"],
                     @catalog.all_nodes)
      end
    end
  end
end
