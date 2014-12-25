# -*- coding: utf-8 -*-
#
# Copyright (C) 2013-2014 Droonga Project
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

require "time"
require "fileutils"
require "droonga/engine/version"
require "droonga/loggable"
require "droonga/engine_state"
require "droonga/catalog_loader"
require "droonga/dispatcher"
require "droonga/file_observer"
require "droonga/live_nodes_list_loader"
require "droonga/node_metadata"

module Droonga
  class Engine
    include Loggable

    attr_writer :on_ready
    def initialize(loop, name, internal_name)
      @state = EngineState.new(loop, name, internal_name)
      @state.cluster.live_nodes_list = load_live_nodes_list
      @catalog = load_catalog
      @state.catalog = @catalog
      @dispatcher = create_dispatcher
      @live_nodes_list_observer = FileObserver.new(loop, Path.live_nodes_list)
      @live_nodes_list_observer.on_change = lambda do
        @state.cluster.live_nodes_list = load_live_nodes_list
      end
      @node_metadata_observer = FileObserver.new(loop, Path.node_metadata)
      @node_metadata_observer.on_change = lambda do
        logger.trace("reloading node_metadata: start")
        node_metadata.reload
        logger.trace("reloading node_metadata: done")
      end
      @on_ready = nil
    end

    def start
      logger.trace("start: start")
      @state.on_ready = lambda do
        @on_ready.call if @on_ready
      end
      @state.start
      @live_nodes_list_observer.start
      @node_metadata_observer.start
      @dispatcher.start
      logger.trace("start: done")
    end

    def stop_gracefully
      logger.trace("stop_gracefully: start")
      @live_nodes_list_observer.stop
      @node_metadata_observer.stop
      on_finish = lambda do
        logger.trace("stop_gracefully/on_finish: start")
        save_last_processed_message_timestamp
        @dispatcher.stop_gracefully do
          @state.shutdown
          yield
        end
        logger.trace("stop_gracefully/on_finish: done")
      end
      if @state.have_session?
        logger.trace("stop_gracefully/having sessions")
        @state.on_finish = on_finish
      else
        logger.trace("stop_gracefully/no session")
        on_finish.call
      end
      logger.trace("stop_gracefully: done")
    end

    # It may be called after stop_gracefully.
    def stop_immediately
      logger.trace("stop_immediately: start")
      save_last_processed_message_timestamp
      @live_nodes_list_observer.stop
      @node_metadata_observer.stop
      @dispatcher.stop_immediately
      @state.shutdown
      logger.trace("stop_immediately: done")
    end

    def process(message)
      return unless effective_message?(message)
      @last_processed_message_timestamp = message["date"]
      @dispatcher.process_message(message)
    end

    def node_metadata
      @node_metadata ||= NodeMetadata.new
    end

    private
    def load_catalog
      catalog_path = Path.catalog
      loader = CatalogLoader.new(catalog_path.to_s)
      catalog = loader.load
      logger.info("catalog loaded",
                  :path  => catalog_path.to_s,
                  :mtime => catalog_path.mtime)
      catalog
    end

    def load_live_nodes_list
      path = Path.live_nodes_list
      loader = LiveNodesListLoader.new(path)
      live_nodes_list = loader.load
      logger.info("live-nodes-list loaded",
                  :path  => path.to_s,
                  :mtime => path.mtime)
      live_nodes_list
    end

    def create_dispatcher
      Dispatcher.new(@state, @catalog)
    end

    def save_last_processed_message_timestamp
      logger.trace("output_last_processed_message_timestamp: start")
      if @last_processed_message_timestamp
        node_metadata.set(:last_processed_message_timestamp, @last_processed_message_timestamp.to_s)
      end
      logger.trace("output_last_processed_message_timestamp: done")
    end

    def effective_message?(message)
      effective_timestamp = effective_message_timestamp
      return true if effective_timestamp.nil?

      message_timestamp = Time.parse(message["date"])
      logger.trace("checking effective_message_timestamp (#{effective_timestamp}) vs message_timestamp(message_timestamp)")
      return false if effective_timestamp >= message_timestamp

      logger.trace("deleting obsolete effective_message_timestamp: start")
      node_metadata.delete(:effective_message_timestamp)
      logger.trace("deleting obsolete effective_message_timestamp: done")
      true
    end

    def effective_message_timestamp
      timestamp = node_metadata.get(:effective_message_timestamp)
      return nil unless timestamp

      begin
        Time.parse(timestamp)
      rescue ArgumentError
        nil
      end
    end

    def log_tag
      "engine"
    end
  end
end
