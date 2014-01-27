# Copyright (C) 2013 Droonga Project
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
  def setup
    data = JSON.parse(File.read(catalog_path))
    @catalog = Droonga::Catalog::Version1.new(data, base_path)
  end

  def test_option
    assert_equal(["for_global"], @catalog.option("plugins"))
  end

  def test_get_partitions
    partitions = @catalog.get_partitions("localhost:23003/test")
    assert_equal({
                   "localhost:23003/test.000" => {
                     :database  => "#{base_path}/000/db",
                     :handlers  => ["for_dataset"],
                     :n_workers => 0
                   },
                   "localhost:23003/test.001" => {
                     :database  => "#{base_path}/001/db",
                     :handlers  => ["for_dataset"],
                     :n_workers => 0
                   },
                   "localhost:23003/test.002" => {
                     :database  => "#{base_path}/002/db",
                     :handlers  => ["for_dataset"],
                     :n_workers => 0
                   },
                   "localhost:23003/test.003" => {
                     :database  => "#{base_path}/003/db",
                     :handlers  => ["for_dataset"],
                     :n_workers => 0
                   },
                 },
                 partitions)
  end

  private
  def fixture_path(base_path)
    File.expand_path("../../fixtures/#{base_path}", __FILE__)
  end

  def catalog_path
    @catalog_path ||= fixture_path("catalog/version1.json")
  end

  def base_path
    File.dirname(catalog_path)
  end

  class DataSetTest < self
    class RingTest < self
      class TotalWeightTest < self
        def setup
        end

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
          catalog = Droonga::Catalog::Version1.new({"datasets" => []},
                                                   "base-path")
          catalog.send(:compute_total_weight, dataset)
        end
      end
    end
  end
end
