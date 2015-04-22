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

module Droonga
  class Cluster
    include Loggable
    include Changable

    class NoCatalogLoaded < StandardError
    end

    class NotStartedYet < StandardError
    end

    class UnknownTarget < StandardError
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

      @params = params
      @catalog = params[:catalog]
      @state = nil

      reload
    end

    def start_observe
      return if @file_observer
      logger.trace("start_observe: start")
      @file_observer = FileObserver.new(@loop, Path.cluster_state)
      @file_observer.on_change = lambda do
        reload
      end
      @file_observer.start
      logger.trace("start_observe: done")
    end

    def stop_observe
      return unless @file_observer
      logger.trace("stop_observe: start")
      @file_observer.stop
      @file_observer = nil
      logger.trace("stop_observe: done")
    end

    def start
      logger.trace("start: start")
      engine_nodes.each(&:start)
      start_observe
      logger.trace("start: done")
    end

    def shutdown
      logger.trace("shutdown: start")
      stop_observe
      engine_nodes.each(&:shutdown)
      logger.trace("shutdown: done")
    end

    def refresh_connection_for(name)
      engine_nodes.each do |node|
        if node.name == name
          node.refresh_connection
        end
      end
    end

    def reload
      logger.trace("reload: start")
      if @state
        old_state = @state.dup
      else
        old_state = nil
      end
      @state = self.class.load_state_file
      if @state == old_state
        logger.info("cluster state not changed")
      else
        logger.info("cluster state changed", :state => @state)
        clear_cache
        engine_nodes.each(&:resume)
        on_change
      end
      logger.trace("reload: done")
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
      sorted_nodes_status = {}
      nodes_status.keys.sort.each do |key|
        sorted_nodes_status[key] = nodes_status[key]
      end
      sorted_nodes_status
    end

    def forward(message, destination)
      receiver = destination["to"]
      receiver_node_name = receiver.match(/\A[^:]+:\d+\/[^.]+/).to_s
      raise NotStartedYet unless @engine_nodes
      @engine_nodes.each do |node|
        if node.name == receiver_node_name
          node.forward(message, destination)
          return
        end
      end
      raise UnknownTarget.new(receiver)
    end

    def engine_node_names
      @engine_node_names ||= engine_nodes.collect(&:name)
    end

    def readable_nodes
      @readable_nodes ||= engine_nodes.select do |node|
        node.readable?
      end.collect(&:name)
    end

    def writable_nodes
      @writable_nodes ||= engine_nodes.select do |node|
        node.writable?
      end.collect(&:name)
    end

    private
    def clear_cache
      @engine_nodes.each(&:shutdown) if @engine_nodes
      @engine_nodes   = nil
      @engine_node_names = nil
      @readable_nodes = nil
      @writable_nodes = nil
    end

    def all_node_names
      raise NoCatalogLoaded.new unless @catalog
      @catalog.all_nodes
    end

    def create_engine_nodes
      all_node_names.collect do |name|
        node_state = @state[name] || {}
        EngineNode.new(@loop,
                       name,
                       node_state,
                       :auto_close_timeout =>
                         @params[:internal_connection_lifetime])
      end
    end

    def log_tag
      "cluster_state"
    end
  end
end
