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

require "droonga/catalog/replica_collection"

class CatalogReplicaCollectionTest < Test::Unit::TestCase
  private
  def create_replica_collection(replicas)
    Droonga::Catalog::ReplicaCollection.new(replicas)
  end

  class SelectTest < self
    def setup
      replicas = [
        "replica1",
        "replica2",
        "replica3",
      ]
      @collection = create_replica_collection(replicas)
    end

    def test_top
      assert_equal(["replica1"], @collection.select(:top))
    end

    def test_random
      random_replicas = @collection.select(:random).collect do |replica|
        replica.gsub(/\Areplica[123]\z/, "any replica")
      end
      assert_equal(["any replica"], random_replicas)
    end

    def test_all
      assert_equal(["replica1", "replica2", "replica3"],
                   @collection.select(:all))
    end
  end
end
