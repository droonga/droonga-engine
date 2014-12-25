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

require "droonga/loggable"
require "droonga/node_metadata"

module Droonga
  class ClusterState
    include Loggable

    attr_accessor :catalog

    def initialize
      @catalog = nil
      @live_nodes_list = nil
    end

    def all_nodes
      @catalog.all_nodes
    end

    def dead_nodes
      if @live_nodes_list
        @live_nodes_list.dead_nodes
      else
        []
      end
    end

    def service_provider_nodes
      if @live_nodes_list
        @live_nodes_list.service_provider_nodes
      else
        all_nodes
      end
    end

    def absorb_source_nodes
      if @live_nodes_list
        @live_nodes_list.absorb_source_nodes
      else
        []
      end
    end

    def absorb_destination_nodes
      if @live_nodes_list
        @live_nodes_list.absorb_destination_nodes
      else
        []
      end
    end

    def same_role_nodes
      case node_metadata.role
      when NodeStatus::Role::SERVICE_PROVIDER
        all_nodes & service_provider_nodes
      when NodeStatus::Role::ABSORB_SOURCE
        all_nodes & absorb_source_nodes
      when NodeStatus::Role::ABSORB_DESTINATION
        all_nodes & absorb_destination_nodes
      else
        []
      end
    end

    def forwardable_nodes
      same_role_nodes - dead_nodes
    end

    private
    def node_metadata
      @node_metadata ||= NodeMetadata.new
    end

    def log_tag
      "cluster_state"
    end
  end
end
