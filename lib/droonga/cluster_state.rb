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
require "droonga/file_observer"
require "droonga/node_metadata"

module Droonga
  class ClusterState
    include Loggable

    attr_accessor :catalog
    attr_writer :on_change

    def initialize(loop)
      @loop = loop

      @catalog = nil
      @state = nil
      @on_change = nil

      @file_observer = FileObserver.new(loop, Path.cluster_state)
      @file_observer.on_change = lambda do
        reload
      end

      reload
    end

    def start_observe
      @file_observer.start
    end

    def stop_observe
      @file_observer.stop
    end

    def reload
      old_state = to_hash
      clear_cache
      @state = state
      logger.info("live-nodes-list loaded")
      unless to_hash == old_state
        on_change
      end
    end

    def all_nodes
      if @catalog
        @catalog.all_nodes
      else
        []
      end
    end

    def dead_nodes
      if @state
        @dead_nodes ||= collect_dead_nodes
      else
        []
      end
    end

    def service_provider_nodes
      if @state
        @service_provider_nodes ||= collect_nodes_by_role(NodeMetadata::Role::SERVICE_PROVIDER)
      else
        all_nodes
      end
    end

    def absorb_source_nodes
      if @state
        @absorb_source_nodes ||= collect_nodes_by_role(NodeMetadata::Role::ABSORB_SOURCE)
      else
        []
      end
    end

    def absorb_destination_nodes
      if @state
        @absorb_destination_nodes ||= collect_nodes_by_role(NodeMetadata::Role::ABSORB_DESTINATION)
      else
        []
      end
    end

    def same_role_nodes
      case node_metadata.role
      when NodeMetadata::Role::SERVICE_PROVIDER
        all_nodes & service_provider_nodes
      when NodeMetadata::Role::ABSORB_SOURCE
        all_nodes & absorb_source_nodes
      when NodeMetadata::Role::ABSORB_DESTINATION
        all_nodes & absorb_destination_nodes
      else
        []
      end
    end

    def forwardable_nodes
      same_role_nodes - dead_nodes
    end

    def writable_nodes
      case node_metadata.role
      when NodeMetadata::Role::SERVICE_PROVIDER
        all_nodes
      when NodeMetadata::Role::ABSORB_SOURCE
        all_nodes & absorb_source_nodes
      when NodeMetadata::Role::ABSORB_DESTINATION
        all_nodes & absorb_destination_nodes
      else
        []
      end
    end

    def unwritable_node?(node_name)
      case node_metadata.role
      when NodeMetadata::Role::SERVICE_PROVIDER
        absorb_source_nodes.include?(node_name) or
          absorb_destination_nodes.include?(node_name)
      when NodeMetadata::Role::ABSORB_SOURCE
        absorb_destination_nodes.include?(node_name)
      else
        false
      end
    end

    def on_change
      @on_change.call if @on_change
    end

    private
    def to_hash
      return nil unless @state

      {
        :all                => @state.keys,
        :dead               => dead_nodes,
        :service_provider   => service_provider_nodes,
        :absorb_source      => absorb_source_nodes,
        :absorb_destination => absorb_destination_nodes,
      }
    end

    def clear_cache
      @dead_nodes = nil
      @service_provider_nodes = nil
      @absorb_source_nodes = nil
      @absorb_destination_nodes = nil
    end

    def state
      path = Path.cluster_state

      return default_state unless path.exist?

      contents = path.read
      return default_state if contents.empty?

      begin
        JSON.parse(contents)
      rescue JSON::ParserError
        default_state
      end
    end

    def default_state
      {}
    end

    def collect_dead_nodes
      nodes = []
      @state.each do |name, state|
        unless state["live"]
          nodes << name
        end
      end
      nodes.sort
    end

    def collect_nodes_by_role(role)
      nodes = []
      @state.each do |name, state|
        if not state["foreign"] and
             state["tags"]["type"] == "engine" and
             state["tags"]["role"] == role
          nodes << name
        end
      end
      nodes.sort
    end

    def node_metadata
      @node_metadata ||= NodeMetadata.new
    end

    def log_tag
      "cluster_state"
    end
  end
end
