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

require "droonga/node_status"

module Droonga
  class LiveNodesList
    def initialize(nodes)
      @nodes = nodes
    end

    def all_nodes
      @all_nodes ||= @nodes.keys.sort
    end

    def dead_nodes
      @dead_nodes ||= collect_dead_nodes
    end

    def absorb_source_nodes
      @absorb_source_nodes ||= collect_absorb_source_nodes
    end

    def absorb_destination_nodes
      @absorb_destination_nodes ||= collect_absorb_destination_nodes
    end

    def service_provider_nodes
      @service_provider_nodes ||= collect_service_provider_nodes
    end

    def ==(nodes_list)
      nodes_list.is_a?(self.class) and
        nodes_list.all_nodes == all_nodes and
        nodes_list.dead_nodes == dead_nodes and
        nodes_list.absorb_source_nodes == absorb_source_nodes and
        nodes_list.absorb_destination_nodes == absorb_destination_nodes and
        nodes_list.service_provider_nodes == service_provider_nodes
    end

    private
    def collect_dead_nodes
      nodes = []
      @nodes.each do |name, state|
        unless state["live"]
          nodes << name
        end
      end
      nodes.sort
    end

    def collect_nodes_by_role(role)
      nodes = []
      @nodes.each do |name, state|
        if not state["foreign"] and
             state["tags"]["role"] == role
          nodes << name
        end
      end
      nodes.sort
    end

    def collect_service_provider_nodes
      collect_nodes_by_role(NodeStatus::Role::SERVICE_PROVIDER)
    end

    def collect_absorb_source_nodes
      collect_nodes_by_role(NodeStatus::Role::ABSORB_SOURCE)
    end

    def collect_absorb_destination_nodes
      collect_nodes_by_role(NodeStatus::Role::ABSORB_DESTINATION)
    end
  end
end
