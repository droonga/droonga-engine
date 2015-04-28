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
require "droonga/file_observer"
require "droonga/safe_file_writer"

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
      @cluster.shutdown
      on_finish = lambda do
        logger.trace("stop_gracefully: middle")
        @last_message_timestamp_observer.stop
        @dispatcher.stop_gracefully do
          #XXX We must save last processed message timstamp
          #    based on forwarded/dispatched messages while
          #    "graceful stop" operations.
          export_last_message_timestamp_to_file
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
      @dispatcher.stop_immediately
      export_last_message_timestamp_to_file
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

    MICRO_SECONDS_DECIMAL_PLACE = 6

    def export_last_message_timestamp_to_cluster
      logger.trace("export_last_message_timestamp_to_cluster: start")
      @last_message_timestamp ||= read_last_message_timestamp_file
      if @last_message_timestamp
        timestamp = @last_message_timestamp
        old_timestamp = read_last_message_timestamp_tag
        logger.trace("export_last_message_timestamp_to_cluster: check",
                     :old     => old_timestamp,
                     :current => @last_message_timestamp)
        if old_timestamp.nil? or timestamp > old_timestamp
          timestamp = timestamp.utc.iso8601(MICRO_SECONDS_DECIMAL_PLACE)
          serf.last_message_timestamp = timestamp
          logger.info("exported last processed message timestamp",
                      :timestamp => timestamp)
        end
      end
      logger.trace("export_last_message_timestamp_to_cluster: done")
    end

    def export_last_message_timestamp_to_file
      logger.trace("export_last_message_timestamp_to_file: start")
      old_timestamp = read_last_message_timestamp_file
      logger.trace("export_last_message_timestamp_to_file: check",
                   :loaded  => old_timestamp,
                   :current => @last_message_timestamp)
      if old_timestamp and
           old_timestamp > @last_message_timestamp
        logger.trace("export_last_message_timestamp_to_file: skipped")
        return
      end
      path = Path.last_message_timestamp
      SafeFileWriter.write(path) do |output, file|
        timestamp = @last_message_timestamp
        timestamp = timestamp.utc.iso8601(MICRO_SECONDS_DECIMAL_PLACE)
        output.puts(timestamp)
      end
      logger.trace("export_last_message_timestamp_to_file: done")
    end

    def run_last_message_timestamp_observer
      path = Path.last_message_timestamp
      observer = FileObserver.new(@loop, path)
      observer.on_change = lambda do
        timestamp = read_last_message_timestamp_file
        logger.trace("last message stamp file is modified",
                     :loaded  => timestamp,
                     :current => @last_message_timestamp)
        if timestamp
          if @last_message_timestamp.nil? or
               timestamp > @last_message_timestamp
            @last_message_timestamp = timestamp
          end
        end
        export_last_message_timestamp_to_cluster
      end
      observer.start
      observer
    end

    def read_last_message_timestamp_file
      file = Path.last_message_timestamp
      return nil unless file.exist?
      timestamp = file.read
      return nil if timestamp.nil? or timestamp.empty?
      Time.parse(timestamp)
    end

    def read_last_message_timestamp_tag
      old_timestamp = serf.last_message_timestamp
      old_timestamp = Time.parse(old_timestamp) if old_timestamp
      old_timestamp
    end

    def serf
      @serf ||= Serf.new(@name.to_s)
    end

    def log_tag
      "engine"
    end
  end
end
