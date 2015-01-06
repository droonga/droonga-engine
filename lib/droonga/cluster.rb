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
require "droonga/path"
require "droonga/file_observer"
require "droonga/engine_node"
require "droonga/node_metadata"

module Droonga
  class Cluster
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
      if @state
        old_state = @state.dup
      else
        old_state = nil
      end
      clear_cache
      @state = load_state_file
      if @state == old_state
        logger.info("cluster state not changed")
      else
        logger.info("cluster state changed")
        engine_nodes.each(&:on_change)
        on_change
      end
    end

    def engine_nodes
      @engine_nodes ||= create_engine_nodes
    end

    def engine_nodes_status
      nodes_status = {}
      engine_nodes.each do |node|
        nodes_status[node.name] = {
          "status" => node.status,
        }
      end
      nodes_status
    end

    def forward(message, destination)
      receiver = destination["to"]
      receiver_node_name = receiver.match(/\A[^:]+:\d+\/[^.]+/).to_s
      @engine_nodes.each do |node|
        if node.name == receiver_node_name
          node.forward(message, destination)
          return true
        end
      end
      false
    end

    def all_nodes
      if @catalog
        @catalog.all_nodes
      else
        []
      end
    end

    def dead_nodes
      @dead_nodes ||= engine_nodes.select do |node|
        node.dead?
      end.collect(&:name)
    end

    def service_provider_nodes
      @service_provider_nodes ||= engine_nodes.select do |node|
        node.service_provider?
      end.collect(&:name)
    end

    def absorb_source_nodes
      @absorb_source_nodes ||= engine_nodes.select do |node|
        node.absorb_source?
      end.collect(&:name)
    end

    def absorb_destination_nodes
      @absorb_destination_nodes ||= engine_nodes.select do |node|
        node.absorb_destination?
      end.collect(&:name)
    end

    def same_role_nodes
      @same_role_nodes ||= engine_nodes.select do |node|
        node.role == node_metadata.role
      end.collect(&:name)
    end

    def forwardable_nodes
      @forwardable_nodes ||= engine_nodes.select do |node|
        node.forwardable?
      end.collect(&:name)
    end

    def writable_nodes
      @writable_nodes ||= engine_nodes.select do |node|
        node.writable?
      end.collect(&:name)
    end

    def on_change
      @on_change.call if @on_change
    end

    private
    def clear_cache
      @engine_nodes = nil
      @dead_nodes = nil
      @service_provider_nodes = nil
      @absorb_source_nodes = nil
      @absorb_destination_nodes = nil
      @same_role_nodes = nil
      @forwardable_nodes = nil
      @writable_nodes = nil
    end

    def load_state_file
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

    def all_node_names
      if @catalog
        @catalog.all_nodes
      else
        []
      end
    end

    def create_engine_nodes
      all_node_names.collect do |name|
        node_state = @state[name] || {}
        EngineNode.new(name, node_state, node_metadata.role, @loop)
      end
    end

    def default_state
      {}
    end

    def node_metadata
      @node_metadata ||= NodeMetadata.new
    end

    def log_tag
      "cluster_state"
    end
  end
end
