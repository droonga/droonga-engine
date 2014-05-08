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

require "droonga/engine/version"
require "droonga/loggable"
require "droonga/engine_state"
require "droonga/catalog_observer"
require "droonga/dispatcher"

module Droonga
  class Engine
    include Loggable

    def initialize(loop, name)
      @state = EngineState.new(loop, name)
      observer = Droonga::CatalogObserver.new(@state.loop)
      @catalog_observer = observer
      @catalog_observer.on_reload = lambda do |catalog|
        graceful_restart(catalog)
        logger.info("restarted")
      end
    end

    def start
      logger.trace("start: start")
      @state.start
      @catalog_observer.start
      catalog = @catalog_observer.catalog
      @dispatcher = create_dispatcher(catalog)
      @dispatcher.start
      logger.trace("start: done")
    end

    def shutdown
      logger.trace("shutdown: start")
      @catalog_observer.stop
      @dispatcher.shutdown
      @state.shutdown
      logger.trace("shutdown: done")
    end

    def process(message)
      @dispatcher.process_message(message)
    end

    private
    def create_dispatcher(catalog)
      Dispatcher.new(@state, catalog)
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

    def log_tag
      "engine"
    end
  end
end
