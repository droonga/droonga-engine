# Copyright (C) 2014-2015 Droonga Project
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

require "droonga/cluster"
require "droonga/node_role"

class ClusterTest < Test::Unit::TestCase
  class StubCatalog
    attr_accessor :all_nodes

    def initialize(all_nodes)
      @all_nodes = all_nodes || []
    end
  end

  class SilentEngineNode < Droonga::EngineNode
    def resume
    end

    private
    def sender_role
      Droonga::NodeRole::SERVICE_PROVIDER
    end

    def create_buffer
      []
    end
  end

  class Cluster < Droonga::Cluster
    def reload
    end

    private
    def create_engine_node(params)
      SilentEngineNode.new(params)
    end
  end

  def create_cluster(options={})
    catalog = StubCatalog.new(options[:all_nodes])
    Cluster.new(:state   => options[:state],
                :catalog => catalog)
  end

  def test_engine_nodes
    cluster = create_cluster(:all_nodes => [
                               "node29:2929/droonga",
                               "node30:2929/droonga",
                             ])
    assert_equal([
                   {:class => SilentEngineNode,
                    :name  => "node29:2929/droonga"},
                   {:class => SilentEngineNode,
                    :name  => "node30:2929/droonga"},
                 ],
                 cluster.engine_nodes.collect do |node|
                   {:class => node.class,
                    :name  => node.name}
                 end)
  end

  def test_engine_node_names
    cluster = create_cluster(:all_nodes => [
                               "node29:2929/droonga",
                               "node30:2929/droonga",
                             ])
    assert_equal([
                   "node29:2929/droonga",
                   "node30:2929/droonga",
                 ],
                 cluster.engine_node_names)
  end

  def test_engine_nodes_status
    cluster = create_cluster(:state => {
                               "node29:2929/droonga" => {
                                 "live" => true,
                                 "role" => Droonga::NodeRole::SERVICE_PROVIDER,
                               },
                               "node30:2929/droonga" => {
                                 "live" => true,
                                 "role" => Droonga::NodeRole::SERVICE_PROVIDER,
                               },
                             },
                             :all_nodes => [
                               "node29:2929/droonga",
                               "node30:2929/droonga",
                             ])
    assert_equal({
                   "node29:2929/droonga" => {
                     "status" => "active",
                   },
                   "node30:2929/droonga" => {
                     "status" => "active",
                   },
                 },
                 cluster.engine_nodes_status)
  end

  def test_readable_nodes
    cluster = create_cluster(:state => {
                               "node29:2929/droonga" => {
                                 "live" => true,
                                 "role" => Droonga::NodeRole::SERVICE_PROVIDER,
                               },
                               "node30:2929/droonga" => {
                                 "live" => false,
                                 "role" => Droonga::NodeRole::SERVICE_PROVIDER,
                               },
                             },
                             :all_nodes => [
                               "node29:2929/droonga",
                               "node30:2929/droonga",
                             ])
    assert_equal([
                   "node29:2929/droonga",
                 ],
                 cluster.readable_nodes)
  end

  def test_writable_nodes
    cluster = create_cluster(:all_nodes => [
                               "node29:2929/droonga",
                               "node30:2929/droonga",
                             ])
    assert_equal([
                   "node29:2929/droonga",
                   "node30:2929/droonga",
                 ],
                 cluster.writable_nodes)
  end

  def test_forward
  end

  def test_bounce
  end
end
