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

require "droonga/loggable"
require "droonga/changable"
require "droonga/path"
require "droonga/file_observer"
require "droonga/engine_node"
require "droonga/node_metadata"

module Droonga
  class Cluster
    include Loggable
    include Changable

    class NoCatalogLoaded < StandardError
    end

    class << self
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

      def default_state
        {}
      end
    end

    attr_accessor :catalog

    def initialize(loop, params)
      @loop = loop

      @catalog = params[:catalog]
      @state = nil
      @node_metadata = params[:metadata]

      reload
    end

    def start_observe
      return if @file_observer
      @file_observer = FileObserver.new(@loop, Path.cluster_state)
      @file_observer.on_change = lambda do
        reload
      end
      @file_observer.start
    end

    def stop_observe
      return unless @file_observer
      @file_observer.stop
      @file_observer = nil
    end

    def start
      engine_nodes.each do |node|
        node.start
      end
      start_observe
    end

    def shutdown
      stop_observe
      engine_nodes.each do |node|
        node.shutdown
      end
    end

    def reload
      if @state
        old_state = @state.dup
      else
        old_state = nil
      end
      clear_cache
      @state = self.class.load_state_file
      if @state == old_state
        logger.info("cluster state not changed")
      else
        logger.info("cluster state changed: #{JSON.generate(@state)}")
        engine_nodes.each(&:resume)
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

    private
    def clear_cache
      @engine_nodes      = nil
      @forwardable_nodes = nil
      @writable_nodes    = nil
    end

    def all_node_names
      raise NoCatalogLoaded.new unless @catalog
      @catalog.all_nodes
    end

    def create_engine_nodes
      all_node_names.collect do |name|
        node_state = @state[name] || {}
        EngineNode.new(name,
                       node_state,
                       @loop,
                       :metadata => @node_metadata)
      end
    end

    def log_tag
      "cluster_state"
    end
  end
end
