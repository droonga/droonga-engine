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

require "droonga/catalog/volume"
require "droonga/catalog/replicas_volume"

class CatalogReplicasTest < Test::Unit::TestCase
  private
  def create_replicas(data_or_replicas)
    Droonga::Catalog::ReplicasVolume.new(nil, data_or_replicas)
  end

  class SelectTest < self
    def setup
      volumes = [
        "volume1",
        "volume2",
        "volume3",
      ]
      @collection = create_replicas(volumes)
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

  class NodesTest < self
    def create_replicas(raw_replicas)
      replicas = raw_replicas.collect do |replica|
        create_single_volume(replica)
      end
      super(replicas)
    end

    def create_single_volume(data)
      Droonga::Catalog::SingleVolume.new(data)
    end

    def setup
      volumes = [
        { "address" => "volume1:10047/droonga.000" },
        { "address" => "volume1:10047/droonga.001" },
        { "address" => "volume2:10047/droonga.002" },
        { "address" => "volume2:10047/droonga.003" },
      ]
      @collection = create_replicas(volumes)
    end

    def test_all_nodes
      assert_equal(["volume1:10047/droonga", "volume2:10047/droonga"],
                   @collection.all_nodes)
    end
  end
end
