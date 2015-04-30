# Copyright (C) 2015 Droonga Project
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

require "droonga/engine_node"
require "droonga/node_role"

class EngineNodeTest < Test::Unit::TestCase
  def node(params)
    Droonga::EngineNode.new(params)
  end

  data(:no_state => {
         :expected => Droonga::NodeRole::SERVICE_PROVIDER,
         :state    => nil,
       },
       :valid => {
         :expected => Droonga::NodeRole::ABSORB_SOURCE,
         :state    => {
           "role" => Droonga::NodeRole::ABSORB_SOURCE,
         },
       },
       :invalid => {
         :expected => Droonga::NodeRole::SERVICE_PROVIDER,
         :state    => {
           "role" => "unknown",
         },
       })
  def test_role(data)
    assert_equal(data[:expected],
                 node(:name => "node29:2929/droonga",
                      :state => data[:state]).role)
  end

  data(:no_state => nil,
       :live_info => {
         "live" => true,
       })
  def test_live(state)
    assert_true(node(:name => "node29:2929/droonga",
                     :state => state).live?)
  end

  data(:no_live_info => {
       },
       :live_info => {
         "live" => false,
       })
  def test_not_live(state)
    assert_false(node(:name => "node29:2929/droonga",
                      :state => state).live?)
  end

  data(:valid => {
         :state => {
           "live" => true,
           "role" => Droonga::NodeRole::SERVICE_PROVIDER,
         },
         :expected => {
           "name"   => "node29:2929/droonga",
           "live"   => true,
           "role"   => Droonga::NodeRole::SERVICE_PROVIDER,
           "status" => "active",
         },
       })
  def test_to_json(data)
    json = node(:name => "node29:2929/droonga",
                :state => data[:state]).to_json
    assert_equal(data[:expected], json)
  end

  class FromServiceProvider < self
    class EngineNode < Droonga::EngineNode
      private
      def sender_role
        Droonga::NodeRole::SERVICE_PROVIDER
      end
    end

    class ToServiceProvider < self
      def node(params)
        EngineNode.new(params)
      end

      data(:same_role => {
             "live" => true,
             "role" => Droonga::NodeRole::SERVICE_PROVIDER,
           })
      def test_forwardable(state)
        assert_true(node(:name => "node29:2929/droonga",
                         :state => state).forwardable?)
      end

      data(:dead => { "live" => false })
      def test_not_forwardable(state)
        assert_false(node(:name => "node29:2929/droonga",
                          :state => state).forwardable?)
      end
    end
  end
end
