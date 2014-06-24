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
require "droonga/live_nodes_list_loader"

module Droonga
  class Engine
    include Loggable

    LAST_PROCESSED_TIMESTAMP = "last-processed.timestamp"
    EFFECTIVE_MESSAGE_TIMESTAMP = "effective-message.timestamp"

    def initialize(loop, name, internal_name)
      @state = EngineState.new(loop, name, internal_name)
      @catalog = load_catalog
      @live_nodes = @catalog.all_nodes
      @dispatcher = create_dispatcher
      @live_nodes_list_observer = FileObserver.new(loop, Path.live_nodes)
      @live_nodes_list_observer.on_change = lambda do
        @live_nodes = load_live_nodes
        @dispatcher.live_nodes = @live_nodes if @dispatcher
      end
    end

    def start
      logger.trace("start: start")
      @state.start
      @live_nodes_list_observer.start
      @dispatcher.start
      logger.trace("start: done")
    end

    def stop_gracefully
      logger.trace("stop_gracefully: start")
      @live_nodes_list_observer.stop
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
      @live_nodes_list_observer.stop
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

    def load_live_nodes
      path = Path.live_nodes
      loader = LiveNodesListLoader.new(path)
      live_nodes = loader.load
      logger.info("live-nodes loaded",
                  :path  => path,
                  :mtime => path.mtime)
      live_nodes
    end

    def create_dispatcher
      dispatcher = Dispatcher.new(@state, @catalog)
      dispatcher.live_nodes = @live_nodes
      dispatcher
    end

    def output_last_processed_timestamp
      FileUtils.mkdir_p(File.dirname(last_processed_timestamp_file))
      File.open(last_processed_timestamp_file, "w") do |file|
        file.write(@last_processed_timestamp)
      end
    end

    def last_processed_timestamp_file
      @last_processed_timestamp_file ||= File.join(Droonga::Path.state, LAST_PROCESSED_TIMESTAMP)
    end

    def effective_message?(message)
      effective_timestamp = effective_message_timestamp
      return true if effective_timestamp.nil?

      message_timestamp = Time.parse(message["date"])
      return false if effective_timestamp >= message_timestamp

      FileUtils.rm(effective_message_timestamp_file)
      true
    end

    def effective_message_timestamp
      return nil unless File.exist?(effective_message_timestamp_file)

      timestamp = File.read(effective_message_timestamp_file)
      begin
        Time.parse(timestamp)
      rescue ArgumentError
        nil
      end
    end

    def effective_message_timestamp_file
      @effective_message_timestamp_file ||= File.join(Droonga::Path.state, EFFECTIVE_MESSAGE_TIMESTAMP)
    end

    def log_tag
      "engine"
    end
  end
end
