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

require "droonga/node_role"

class NodeRoleTest < Test::Unit::TestCase
  class NormalizeTest < self
    data(:service_provider => {
           :input    => "service-provider",
           :expected => Droonga::NodeRole::SERVICE_PROVIDER,
         },
         :absorb_source => {
           :input    => "absorb-source",
           :expected => Droonga::NodeRole::ABSORB_SOURCE,
         },
         :absorb_destination => {
           :input    => "absorb-destination",
           :expected => Droonga::NodeRole::ABSORB_DESTINATION,
         },
         :not_valid_for_a_node => {
           :input    => "any",
           :expected => Droonga::NodeRole::SERVICE_PROVIDER,
         },
         :mixed_case => {
           :input    => "Absorb-Source",
           :expected => Droonga::NodeRole::ABSORB_SOURCE,
         },
         :nil => {
           :input    => nil,
           :expected => Droonga::NodeRole::SERVICE_PROVIDER,
         },
         :blank => {
           :input    => "",
           :expected => Droonga::NodeRole::SERVICE_PROVIDER,
         })
    def test_normalize(data)
      assert_equal(data[:expected],
                   Droonga::NodeRole.normalize(data[:input]))
    end
  end
end
