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

  class Buffered < Droonga::EngineNode
    private
    def create_buffer
      [0]
    end
  end

  class NotBuffered < Droonga::EngineNode
    private
    def create_buffer
      []
    end
  end

  class FromServiceProvider < self
    class BufferedEngineNode < Buffered
      private
      def sender_role
        Droonga::NodeRole::SERVICE_PROVIDER
      end
    end

    class NotBufferedEngineNode < NotBuffered
      private
      def sender_role
        Droonga::NodeRole::SERVICE_PROVIDER
      end
    end

    class EngineNode < NotBufferedEngineNode
    end

    data(:same_role => {
           "live" => true,
           "role" => Droonga::NodeRole::SERVICE_PROVIDER,
         })
    def test_forwardable(state)
      assert_true(EngineNode.new(:name => "node29:2929/droonga",
                                 :state => state).forwardable?)
    end

    data(:dead => {
           "live" => false,
           "role" => Droonga::NodeRole::SERVICE_PROVIDER,
         },
         :to_absorb_source => {
           "live" => true,
           "role" => Droonga::NodeRole::ABSORB_SOURCE,
         },
         :to_absorb_destination => {
           "live" => true,
           "role" => Droonga::NodeRole::ABSORB_DESTINATION,
         })
    def test_not_forwardable(state)
      assert_false(EngineNode.new(:name => "node29:2929/droonga",
                                  :state => state).forwardable?)
    end

    data(:same_role_with_no_unprocessed_message => {
           :state => {
             "live" => true,
             "role" => Droonga::NodeRole::SERVICE_PROVIDER,
           },
           :class => NotBufferedEngineNode,
         })
    def test_readable(data)
      assert_true(data[:class].new(:name => "node29:2929/droonga",
                                   :state => data[:state]).readable?)
    end

    data(:dead => {
           :state => {
             "live" => false,
             "role" => Droonga::NodeRole::SERVICE_PROVIDER,
           },
           :class => NotBufferedEngineNode,
         },
         :have_unprocessed_message_in_other_node => {
           :state => {
             "live" => true,
             "role" => Droonga::NodeRole::SERVICE_PROVIDER,
             "have_unprocessed_messages" => true,
           },
           :class => NotBufferedEngineNode,
         },
         :have_unprocessed_message => {
           :state => {
             "live" => true,
             "role" => Droonga::NodeRole::SERVICE_PROVIDER,
           },
           :class => BufferedEngineNode,
         },
         :to_absorb_source => {
           :state => {
             "live" => true,
             "role" => Droonga::NodeRole::ABSORB_SOURCE,
           },
           :class => NotBufferedEngineNode,
         },
         :to_absorb_destination => {
           :state => {
             "live" => true,
             "role" => Droonga::NodeRole::ABSORB_DESTINATION,
           },
           :class => NotBufferedEngineNode,
         })
    def test_not_readable(data)
      assert_false(data[:class].new(:name => "node29:2929/droonga",
                                    :state => data[:state]).readable?)
    end

    data(:same_role => {
           "live" => true,
           "role" => Droonga::NodeRole::SERVICE_PROVIDER,
         },
         :to_absorb_source => {
           "live" => true,
           "role" => Droonga::NodeRole::ABSORB_SOURCE,
         },
         :to_absorb_destination => {
           "live" => true,
           "role" => Droonga::NodeRole::ABSORB_DESTINATION,
         },
         :to_dead => {
           "live" => false,
           "role" => Droonga::NodeRole::SERVICE_PROVIDER,
         },
         :to_node_have_unprocessed_message => {
           "live" => true,
           "role" => Droonga::NodeRole::SERVICE_PROVIDER,
           "have_unprocessed_messages" => true,
         })
    def test_writable(state)
      assert_true(EngineNode.new(:name => "node29:2929/droonga",
                                 :state => state).writable?)
    end

    data(:dead => {
           :state => {
             "live" => false,
           },
           :expected => "dead",
         },
         :readable => {
           :state => {
             "live" => true,
             "role" => Droonga::NodeRole::SERVICE_PROVIDER,
           },
           :expected => "active",
         },
         :not_readable_but_writable => {
           :state => {
             "live" => true,
             "role" => Droonga::NodeRole::ABSORB_SOURCE,
           },
           :expected => "inactive",
         })
    def test_status(data)
      assert_equal(data[:expected],
                   EngineNode.new(:name => "node29:2929/droonga",
                                  :state => data[:state]).status)
    end
  end

  class FromAbsorbSource < self
    class BufferedEngineNode < Buffered
      private
      def sender_role
        Droonga::NodeRole::ABSORB_SOURCE
      end
    end

    class NotBufferedEngineNode < NotBuffered
      private
      def sender_role
        Droonga::NodeRole::ABSORB_SOURCE
      end
    end

    class EngineNode < NotBufferedEngineNode
    end

    data(:same_role => {
           "live" => true,
           "role" => Droonga::NodeRole::ABSORB_SOURCE,
         })
    def test_forwardable(state)
      assert_true(EngineNode.new(:name => "node29:2929/droonga",
                                 :state => state).forwardable?)
    end

    data(:dead => {
           "live" => false,
           "role" => Droonga::NodeRole::ABSORB_SOURCE,
         },
         :to_service_provider => {
           "live" => true,
           "role" => Droonga::NodeRole::SERVICE_PROVIDER,
         },
         :to_absorb_destination => {
           "live" => true,
           "role" => Droonga::NodeRole::ABSORB_DESTINATION,
         })
    def test_not_forwardable(state)
      assert_false(EngineNode.new(:name => "node29:2929/droonga",
                                  :state => state).forwardable?)
    end

    data(:same_role_with_no_unprocessed_message => {
           :state => {
             "live" => true,
             "role" => Droonga::NodeRole::ABSORB_SOURCE,
           },
           :class => NotBufferedEngineNode,
         },
         :have_unprocessed_message_in_other_node => {
           :state => {
             "live" => true,
             "role" => Droonga::NodeRole::ABSORB_SOURCE,
             "have_unprocessed_messages" => true,
           },
           :class => NotBufferedEngineNode,
         })
    def test_readable(data)
      assert_true(data[:class].new(:name => "node29:2929/droonga",
                                   :state => data[:state]).readable?)
    end

    data(:dead => {
           :state => {
             "live" => false,
             "role" => Droonga::NodeRole::ABSORB_SOURCE,
           },
           :class => NotBufferedEngineNode,
         },
         :have_unprocessed_message => {
           :state => {
             "live" => true,
             "role" => Droonga::NodeRole::ABSORB_SOURCE,
           },
           :class => BufferedEngineNode,
         },
         :to_service_provider => {
           :state => {
             "live" => true,
             "role" => Droonga::NodeRole::SERVICE_PROVIDER,
           },
           :class => NotBufferedEngineNode,
         },
         :to_absorb_destination => {
           :state => {
             "live" => true,
             "role" => Droonga::NodeRole::ABSORB_DESTINATION,
           },
           :class => NotBufferedEngineNode,
         })
    def test_not_readable(data)
      assert_false(data[:class].new(:name => "node29:2929/droonga",
                                    :state => data[:state]).readable?)
    end

    data(:same_role => {
           "live" => true,
           "role" => Droonga::NodeRole::ABSORB_SOURCE,
         },
         :to_dead => {
           "live" => false,
           "role" => Droonga::NodeRole::ABSORB_SOURCE,
         },
         :to_node_have_unprocessed_message => {
           "live" => true,
           "role" => Droonga::NodeRole::ABSORB_SOURCE,
           "have_unprocessed_messages" => true,
         })
    def test_writable(state)
      assert_true(EngineNode.new(:name => "node29:2929/droonga",
                                 :state => state).writable?)
    end

    data(:to_service_provider => {
           "live" => true,
           "role" => Droonga::NodeRole::SERVICE_PROVIDER,
         },
         :to_absorb_destination => {
           "live" => true,
           "role" => Droonga::NodeRole::ABSORB_DESTINATION,
         })
    def test_not_writable(state)
      assert_false(EngineNode.new(:name => "node29:2929/droonga",
                                  :state => state).writable?)
    end

    data(:dead => {
           :state => {
             "live" => false,
           },
           :expected => "dead",
         },
         :readable => {
           :state => {
             "live" => true,
             "role" => Droonga::NodeRole::ABSORB_SOURCE,
           },
           :expected => "active",
         },
         :not_readable_but_writable => {
           :state => {
             "live" => true,
             "role" => Droonga::NodeRole::SERVICE_PROVIDER,
           },
           :expected => "inactive",
         })
    def test_status(data)
      assert_equal(data[:expected],
                   EngineNode.new(:name => "node29:2929/droonga",
                                  :state => data[:state]).status)
    end
  end

  class FromAbsorbDestination < self
    class BufferedEngineNode < Buffered
      private
      def sender_role
        Droonga::NodeRole::ABSORB_DESTINATION
      end
    end

    class NotBufferedEngineNode < NotBuffered
      private
      def sender_role
        Droonga::NodeRole::ABSORB_DESTINATION
      end
    end

    class EngineNode < NotBufferedEngineNode
    end

    data(:same_role => {
           "live" => true,
           "role" => Droonga::NodeRole::ABSORB_DESTINATION,
         })
    def test_forwardable(state)
      assert_true(EngineNode.new(:name => "node29:2929/droonga",
                                 :state => state).forwardable?)
    end

    data(:dead => {
           "live" => false,
           "role" => Droonga::NodeRole::ABSORB_DESTINATION,
         },
         :to_service_provider => {
           "live" => true,
           "role" => Droonga::NodeRole::SERVICE_PROVIDER,
         },
         :to_absorb_source => {
           "live" => true,
           "role" => Droonga::NodeRole::ABSORB_SOURCE,
         })
    def test_not_forwardable(state)
      assert_false(EngineNode.new(:name => "node29:2929/droonga",
                                  :state => state).forwardable?)
    end

    data(:same_role_with_no_unprocessed_message => {
           :state => {
             "live" => true,
             "role" => Droonga::NodeRole::ABSORB_DESTINATION,
           },
           :class => NotBufferedEngineNode,
         },
         :have_unprocessed_message_in_other_node => {
           :state => {
             "live" => true,
             "role" => Droonga::NodeRole::ABSORB_DESTINATION,
             "have_unprocessed_messages" => true,
           },
           :class => NotBufferedEngineNode,
         })
    def test_readable(data)
      assert_true(data[:class].new(:name => "node29:2929/droonga",
                                   :state => data[:state]).readable?)
    end

    data(:dead => {
           :state => {
             "live" => false,
             "role" => Droonga::NodeRole::ABSORB_DESTINATION,
           },
           :class => NotBufferedEngineNode,
         },
         :have_unprocessed_message => {
           :state => {
             "live" => true,
             "role" => Droonga::NodeRole::ABSORB_DESTINATION,
           },
           :class => BufferedEngineNode,
         },
         :to_service_provider => {
           :state => {
             "live" => true,
             "role" => Droonga::NodeRole::SERVICE_PROVIDER,
           },
           :class => NotBufferedEngineNode,
         },
         :to_absorb_source => {
           :state => {
             "live" => true,
             "role" => Droonga::NodeRole::ABSORB_SOURCE,
           },
           :class => NotBufferedEngineNode,
         })
    def test_not_readable(data)
      assert_false(data[:class].new(:name => "node29:2929/droonga",
                                    :state => data[:state]).readable?)
    end

    data(:same_role => {
           "live" => true,
           "role" => Droonga::NodeRole::ABSORB_DESTINATION,
         },
         :to_dead => {
           "live" => false,
           "role" => Droonga::NodeRole::ABSORB_DESTINATION,
         },
         :to_node_have_unprocessed_message => {
           "live" => true,
           "role" => Droonga::NodeRole::ABSORB_DESTINATION,
           "have_unprocessed_messages" => true,
         })
    def test_writable(state)
      assert_true(EngineNode.new(:name => "node29:2929/droonga",
                                 :state => state).writable?)
    end

    data(:to_service_provider => {
           "live" => true,
           "role" => Droonga::NodeRole::SERVICE_PROVIDER,
         },
         :to_absorb_source => {
           "live" => true,
           "role" => Droonga::NodeRole::ABSORB_SOURCE,
         })
    def test_not_writable(state)
      assert_false(EngineNode.new(:name => "node29:2929/droonga",
                                  :state => state).writable?)
    end

    data(:dead => {
           :state => {
             "live" => false,
           },
           :expected => "dead",
         },
         :readable => {
           :state => {
             "live" => true,
             "role" => Droonga::NodeRole::ABSORB_DESTINATION,
           },
           :expected => "active",
         },
         :not_readable_but_writable => {
           :state => {
             "live" => true,
             "role" => Droonga::NodeRole::SERVICE_PROVIDER,
           },
           :expected => "inactive",
         })
    def test_status(data)
      assert_equal(data[:expected],
                   EngineNode.new(:name => "node29:2929/droonga",
                                  :state => data[:state]).status)
    end
  end
end
