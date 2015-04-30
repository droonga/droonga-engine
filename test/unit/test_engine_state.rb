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

require "droonga/engine_state"

class EngineStateTest < Test::Unit::TestCase
  class EngineState < Droonga::EngineState
    private
    def create_forwarder
      nil
    end

    def create_replier
      nil
    end
  end

  PUBLIC_NODE_NAME   = "node29:2929/droonga"
  INTERNAL_NODE_NAME = "node29:12929/droonga"

  def setup
    @state = EngineState.new(:name          => PUBLIC_NODE_NAME,
                             :internal_name => INTERNAL_NODE_NAME)
  end

  data(:public => {
         :route    => PUBLIC_NODE_NAME,
         :expected => INTERNAL_NODE_NAME,
       },
       :internal => {
         :route    => INTERNAL_NODE_NAME,
         :expected => INTERNAL_NODE_NAME,
       },
       :public_with_local => {
         :route    => "#{PUBLIC_NODE_NAME}.\#1",
         :expected => "#{INTERNAL_NODE_NAME}.\#1",
       },
       :internal_with_local => {
         :route    => "#{INTERNAL_NODE_NAME}.\#1",
         :expected => "#{INTERNAL_NODE_NAME}.\#1",
       },
       :foreign => {
         :route    => "node30:2929/droonga.\#1",
         :expected => "node30:2929/droonga.\#1",
       })
  def test_internal_route(data)
    assert_equal(data[:expected],
                 @state.internal_route(data[:route]))
  end

  data(:public => {
         :route    => PUBLIC_NODE_NAME,
         :expected => PUBLIC_NODE_NAME,
       },
       :internal => {
         :route    => INTERNAL_NODE_NAME,
         :expected => PUBLIC_NODE_NAME,
       },
       :public_with_local => {
         :route    => "#{PUBLIC_NODE_NAME}.\#1",
         :expected => "#{PUBLIC_NODE_NAME}.\#1",
       },
       :internal_with_local => {
         :route    => "#{INTERNAL_NODE_NAME}.\#1",
         :expected => "#{PUBLIC_NODE_NAME}.\#1",
       },
       :foreign => {
         :route    => "node30:2929/droonga.\#1",
         :expected => "node30:2929/droonga.\#1",
       })
  def test_public_route(data)
    assert_equal(data[:expected],
                 @state.public_route(data[:route]))
  end

  data(:public => {
         :route    => PUBLIC_NODE_NAME,
         :expected => INTERNAL_NODE_NAME,
       },
       :internal => {
         :route    => INTERNAL_NODE_NAME,
         :expected => INTERNAL_NODE_NAME,
       },
       :public_with_local => {
         :route    => "#{PUBLIC_NODE_NAME}.\#1",
         :expected => INTERNAL_NODE_NAME,
       },
       :internal_with_local => {
         :route    => "#{INTERNAL_NODE_NAME}.\#1",
         :expected => INTERNAL_NODE_NAME,
       },
       :foreign => {
         :route    => "node30:2929/droonga.\#1",
         :expected => "node30:2929/droonga",
       })
  def test_internal_farm_path(data)
    assert_equal(data[:expected],
                 @state.internal_farm_path(data[:route]))
  end

  data(:public => {
         :route    => PUBLIC_NODE_NAME,
         :expected => PUBLIC_NODE_NAME,
       },
       :internal => {
         :route    => INTERNAL_NODE_NAME,
         :expected => PUBLIC_NODE_NAME,
       },
       :public_with_local => {
         :route    => "#{PUBLIC_NODE_NAME}.\#1",
         :expected => PUBLIC_NODE_NAME,
       },
       :internal_with_local => {
         :route    => "#{INTERNAL_NODE_NAME}.\#1",
         :expected => PUBLIC_NODE_NAME,
       },
       :foreign => {
         :route    => "node30:2929/droonga.\#1",
         :expected => "node30:2929/droonga",
       })
  def test_public_farm_path(data)
    assert_equal(data[:expected],
                 @state.public_farm_path(data[:route]))
  end
end
