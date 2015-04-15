# -*- coding: utf-8 -*-
#
# Copyright (C) 2013-2015 Droonga Project
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

require "fileutils"

require "droonga/engine/version"
require "droonga/loggable"
require "droonga/deferrable"
require "droonga/engine_state"
require "droonga/cluster"
require "droonga/catalog/loader"
require "droonga/dispatcher"
require "droonga/node_metadata"

module Droonga
  class Engine
    include Loggable
    include Deferrable

    def initialize(loop, name, internal_name)
      @loop = loop
      @catalog = load_catalog
      @node_metadata = NodeMetadata.new
      @state = EngineState.new(loop, name,
                               internal_name,
                               :catalog  => @catalog,
                               :metadata => @node_metadata)
      @cluster = Cluster.new(loop,
                             :catalog  => @catalog,
                             :metadata => @node_metadata)

      @dispatcher = create_dispatcher
    end

    def start
      logger.trace("start: start")
      @node_metadata.start_observe(@loop)
      @state.on_ready = lambda do
        on_ready
      end
      @state.on_failure = lambda do
        on_failure
      end
      @state.start
      @cluster.start
      @dispatcher.start
      logger.trace("start: done")
    end

    def stop_gracefully
      logger.trace("stop_gracefully: start")
      @node_metadata.stop_observe
      @cluster.shutdown
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
      @dispatcher.stop_immediately
      @cluster.shutdown
      @state.shutdown
      logger.trace("stop_immediately: done")
    end

    def process(message)
      @last_processed_message_timestamp = message["date"]
      @dispatcher.process_message(message)
    end

    private
    def load_catalog
      catalog_path = Path.catalog
      loader = Catalog::Loader.new(catalog_path.to_s)
      catalog = loader.load
      logger.info("catalog loaded",
                  :path  => catalog_path.to_s,
                  :mtime => catalog_path.mtime)
      catalog
    end

    def create_dispatcher
      Dispatcher.new(@state, @cluster, @catalog)
    end

    def save_last_processed_message_timestamp
      logger.trace("output_last_processed_message_timestamp: start")
      if @last_processed_message_timestamp
        @node_metadata.set(:last_processed_message_timestamp, @last_processed_message_timestamp.to_s)
      end
      logger.trace("output_last_processed_message_timestamp: done")
    end

    def log_tag
      "engine"
    end
  end
end
