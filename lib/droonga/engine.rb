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
require "time"

require "droonga/engine/version"
require "droonga/loggable"
require "droonga/deferrable"
require "droonga/engine_state"
require "droonga/cluster"
require "droonga/catalog/loader"
require "droonga/dispatcher"
require "droonga/serf"
require "droonga/serf/tag"
require "droonga/timestamp"

module Droonga
  class Engine
    include Loggable
    include Deferrable

    attr_reader :cluster

    def initialize(loop, name, internal_name, options={})
      @name = name
      @internal_name = internal_name
      @loop = loop
      @catalog = load_catalog
      @state = EngineState.new(loop, name,
                               internal_name,
                               :catalog  => @catalog,
                               :internal_connection_lifetime =>
                                 options[:internal_connection_lifetime])
      @cluster = Cluster.new(loop,
                             :catalog  => @catalog,
                             :internal_connection_lifetime =>
                               options[:internal_connection_lifetime])

      @dispatcher = create_dispatcher
      @cluster.on_change = lambda do
        @dispatcher.refresh_node_reference
      end
    end

    def start
      logger.trace("start: start")
      @state.on_ready = lambda do
        on_ready
        serf = Serf.new(@name.to_s)
        serf.set_tag(Serf::Tag.internal_node_name, @internal_name)
      end
      @state.on_failure = lambda do
        on_failure
      end
      @state.start
      @cluster.start
      @dispatcher.start
      @last_message_timestamp_observer = run_last_message_timestamp_observer
      logger.trace("start: done")
    end

    def stop_gracefully
      logger.trace("stop_gracefully: start")
      @last_message_timestamp_observer.stop
      Timestamp.last_message_timestamp = nil # to avoid old timestamp is used
      @cluster.shutdown
      on_finish = lambda do
        logger.trace("stop_gracefully: middle")
        @dispatcher.stop_gracefully do
          #XXX We must save last processed message timstamp
          #    based on forwarded/dispatched messages while
          #    "graceful stop" operations.
          save_last_message_timestamp
          @state.shutdown
          yield
          logger.trace("stop_gracefully: done")
        end
      end
      if @state.have_session?
        logger.trace("stop_gracefully: having sessions")
        @state.on_finish = on_finish
      else
        logger.trace("stop_gracefully: no session")
        on_finish.call
      end
    end

    # It may be called after stop_gracefully.
    def stop_immediately
      logger.trace("stop_immediately: start")
      @last_message_timestamp_observer.stop
      Timestamp.last_message_timestamp = nil # to avoid old timestamp is used
      @dispatcher.stop_immediately
      save_last_message_timestamp
      @cluster.shutdown
      @state.shutdown
      logger.trace("stop_immediately: done")
    end

    def refresh_self_reference
      @cluster.refresh_connection_for(@name)
      @state.forwarder.refresh_connection_for(@name)
    end

    def process(message)
      if message.include?("date")
        date = Time.parse(message["date"])
        if @last_message_timestamp.nil? or
             @last_message_timestamp < date
          @last_message_timestamp = date
        end
      end
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

    def save_last_message_timestamp
      logger.trace("save_last_message_timestamp: start",
                   :current => @last_message_timestamp)
      Timestamp.last_message_timestamp = @last_message_timestamp
      logger.trace("save_last_message_timestamp: done")
    end

    def run_last_message_timestamp_observer
      Timestamp.run_last_message_timestamp_observer(@loop) do |timestamp|
        logger.trace("last message timestamp file is modified",
                     :loaded  => timestamp,
                     :current => @last_message_timestamp)
        if timestamp
          if @last_message_timestamp.nil? or
               timestamp > @last_message_timestamp
            @last_message_timestamp = timestamp
          elsif timestamp < @last_message_timestamp
            Timestamp.last_message_timestamp = @last_message_timestamp
          end
        end
      end
    end

    def log_tag
      "engine"
    end
  end
end
