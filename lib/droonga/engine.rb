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
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

require "time"
require "fileutils"
require "droonga/engine/version"
require "droonga/loggable"
require "droonga/engine_state"
require "droonga/catalog_loader"
require "droonga/dispatcher"
require "droonga/file_observer"
require "droonga/nodes_status_loader"

module Droonga
  class Engine
    include Loggable

    def initialize(loop, name, internal_name)
      @state = EngineState.new(loop, name, internal_name)
      @catalog = load_catalog
      @state.catalog = @catalog

      serf = Serf.new(loop, name)
      serf.set_tag("cluster_id", @state.cluster_id)

      @dispatcher = create_dispatcher
      @nodes_status_observer = FileObserver.new(loop, Path.nodes_status)
      @nodes_status_observer.on_change = lambda do
        @state.nodes_status = load_nodes_status
      end
    end

    def start
      logger.trace("start: start")
      @state.start
      @nodes_status_observer.start
      @dispatcher.start
      logger.trace("start: done")
    end

    def stop_gracefully
      logger.trace("stop_gracefully: start")
      @nodes_status_observer.stop
      on_finish = lambda do
        output_last_processed_timestamp
        @dispatcher.shutdown
        @state.shutdown
        yield
      end
      if @state.have_session?
        @state.on_finish = on_finish
      else
        on_finish.call
      end
      logger.trace("stop_gracefully: done")
    end

    # It may be called after stop_gracefully.
    def stop_immediately
      logger.trace("stop_immediately: start")
      output_last_processed_timestamp
      @nodes_status_observer.stop
      @dispatcher.shutdown
      @state.shutdown
      logger.trace("stop_immediately: done")
    end

    def process(message)
      return unless effective_message?(message)
      @last_processed_timestamp = message["date"]
      @dispatcher.process_message(message)
    end

    private
    def load_catalog
      catalog_path = Path.catalog
      loader = CatalogLoader.new(catalog_path.to_s)
      catalog = loader.load
      logger.info("catalog loaded",
                  :path  => catalog_path,
                  :mtime => catalog_path.mtime)
      catalog
    end

    def load_nodes_status
      path = Path.nodes_status
      loader = NodesStatusLoader.new(path)
      nodes_status = loader.load
      logger.info("nodes-status loaded",
                  :path  => path,
                  :mtime => path.mtime)
      nodes_status
    end

    def create_dispatcher
      Dispatcher.new(@state, @catalog)
    end

    def output_last_processed_timestamp
      path = Path.last_processed_timestamp
      FileUtils.mkdir_p(path.dirname.to_s)
      path.open("w") do |file|
        file.write(@last_processed_timestamp)
      end
    end

    def effective_message?(message)
      effective_timestamp = effective_message_timestamp
      return true if effective_timestamp.nil?

      message_timestamp = Time.parse(message["date"])
      return false if effective_timestamp >= message_timestamp

      FileUtils.rm(Path.effective_timestamp.to_s)
      true
    end

    def effective_message_timestamp
      path = Path.effective_message_timestamp
      return nil unless path.exist?

      timestamp = path.read
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
