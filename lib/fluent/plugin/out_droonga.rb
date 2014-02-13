# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Droonga Project
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

require "droonga/engine"
require "droonga/plugin_loader"
require "droonga/catalog_observer"

module Fluent
  class DroongaOutput < Output
    Plugin.register_output("droonga", self)

    config_param :name, :string, :default => ""

    def start
      super
      Droonga::PluginLoader.load_all
      @catalog_observer = Droonga::CatalogObserver.new
      @catalog_observer.on_reload = lambda do |catalog|
        graceful_engine_restart(catalog)
        $log.info "engine restarted"
      end
      @catalog_observer.start
      catalog = @catalog_observer.catalog
      @engine = create_engine(catalog)
      @engine.start
    end

    def shutdown
      @engine.shutdown
      if @catalog_observer
        @catalog_observer.stop
      end
      super
    end

    def emit(tag, es, chain)
      es.each do |time, record|
        process_event(tag, record)
      end
      chain.next
    end

    private
    def create_engine(catalog)
      Droonga::Engine.new(catalog, :name => @name)
    end

    def graceful_engine_restart(catalog)
      $log.trace "out_droonga: start: graceful_engine_restart"
      old_engine = @engine
      $log.trace "out_droonga: creating new engine"
      new_engine = create_engine(catalog)
      new_engine.start
      @engine = new_engine
      $log.trace "out_droonga: shutdown old engine"
      old_engine.shutdown
      $log.trace "out_droonga: done: graceful_engine_restart"
    end

    def process_event(tag, record)
      $log.trace("out_droonga: tag: <#{tag}>")
      @engine.process(parse_record(tag, record))
    end

    def parse_record(tag, record)
      prefix, type, *arguments = tag.split(/\./)
      if type.nil? || type.empty? || type == "message"
        message = record
      else
        message = {
          "type" => type,
          "arguments" => arguments,
          "body" => record
        }
      end
      reply_to = message["replyTo"]
      if reply_to.is_a? String
        message["replyTo"] = {
          "type" => "#{message["type"]}.result",
          "to" => reply_to
        }
      end
      message
    end
  end
end
