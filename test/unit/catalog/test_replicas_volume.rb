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
  def create_replicas(raw_volume)
    Droonga::Catalog::ReplicasVolume.new(nil, raw_volume)
  end

  class SelectTest < self
    def setup
      volume = {
        "replicas" => [
          { "address" => "volume1:10047/droonga.000" },
          { "address" => "volume2:10047/droonga.000" },
          { "address" => "volume3:10047/droonga.000" },
        ],
      }
      @replicas = create_replicas(volume)
    end

    def test_top
      hosts = @replicas.select(:top).collect do |volume|
        volume.address.host
      end
      assert_equal(["volume1"],
                   hosts)
    end

    def test_random
      random_volumes = @replicas.select(:random).collect do |volume|
        volume.address.host.gsub(/\Avolume[123]\z/, "any volume")
      end
      assert_equal(["any volume"], random_volumes)
    end

    def test_all
      hosts = @replicas.select(:all).collect do |volume|
        volume.address.host
      end
      assert_equal(["volume1", "volume2", "volume3"],
                   hosts)
    end
  end

  class NodesTest < self
    def setup
      volume = {
        "replicas" => [
          { "address" => "volume1:10047/droonga.000" },
          { "address" => "volume1:10047/droonga.001" },
          { "address" => "volume2:10047/droonga.002" },
          { "address" => "volume2:10047/droonga.003" },
        ],
      }
      @replicas = create_replicas(volume)
    end

    def test_all_nodes
      assert_equal(["volume1:10047/droonga", "volume2:10047/droonga"],
                   @replicas.all_nodes)
    end
  end
end
