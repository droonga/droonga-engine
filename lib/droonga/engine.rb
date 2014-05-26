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
require "droonga/catalog_observer"
require "droonga/dispatcher"
require "droonga/live_nodes_list_observer"

module Droonga
  class Engine
    include Loggable

    LAST_PROCESSED_TIMESTAMP = "last_processed_timestamp"
    EFFECTIVE_MESSAGE_TIMESTAMP = "effective_message_timestamp"

    def initialize(loop, name, internal_name)
      @state = EngineState.new(loop, name, internal_name)

      @catalog_observer = Droonga::CatalogObserver.new(@state.loop)
      @catalog_observer.on_reload = lambda do |catalog|
        graceful_restart(catalog)
        logger.info("restarted")
      end

      @live_nodes_list_observer = LiveNodesListObserver.new
      @live_nodes_list_observer.on_update = lambda do |live_nodes|
        @live_nodes = live_nodes
        @dispatcher.live_nodes = live_nodes if @dispatcher
      end
    end

    def start
      logger.trace("start: start")
      @state.start
      @live_nodes_list_observer.start
      @catalog_observer.start
      catalog = @catalog_observer.catalog
      @live_nodes = catalog.all_nodes
      @dispatcher = create_dispatcher(catalog)
      @dispatcher.start
      logger.trace("start: done")
    end

    def shutdown
      logger.trace("shutdown: start")
      output_last_processed_timestamp
      @catalog_observer.stop
      @live_nodes_list_observer.stop
      @dispatcher.shutdown
      @state.shutdown
      logger.trace("shutdown: done")
    end

    def process(message)
      return unless effective_message?(message)
      @last_processed_timestamp = message["date"]
      @dispatcher.process_message(message)
    end

    private
    def create_dispatcher(catalog)
      dispatcher = Dispatcher.new(@state, catalog)
      dispatcher.live_nodes = @live_nodes
      dispatcher
    end

    def graceful_restart(catalog)
      logger.trace("graceful_restart: start")
      old_dispatcher = @dispatcher
      logger.trace("graceful_restart: creating new dispatcher")
      new_dispatcher = create_dispatcher(catalog)
      new_dispatcher.start
      @dispatcher = new_dispatcher
      logger.trace("graceful_restart: shutdown old dispatcher")
      old_dispatcher.shutdown
      logger.trace("graceful_restart: done")
    end

    def output_last_processed_timestamp
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
