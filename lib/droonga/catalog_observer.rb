# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Droonga Project
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

require "droonga/catalog_loader"

module Droonga
  class CatalogObserver
    DEFAULT_CATALOG_PATH = "catalog.json"
    CHECK_INTERVAL = 1

    attr_reader :catalog
    attr_accessor :on_reload

    def initialize
      @catalog_path = catalog_path
      load_catalog!
    end

    def start
      @loop = Cool.io::Loop.new
      attach(@loop)
      @thread = Thread.new do
        @loop.run
      end
    end

    def stop
      @loop.stop
      @thread.join
    end

    def attach(loop)
      watcher = Cool.io::TimerWatcher.new(CHECK_INTERVAL, true)
      observer = self
      watcher.on_timer do
        observer.ensure_latest_catalog_loaded
      end
      loop.attach(watcher)
    end

    def ensure_latest_catalog_loaded
      if catalog_updated?
        load_catalog!
        on_reload.call(catalog) if on_reload
      end
    end

    def catalog_path
      path = ENV["DROONGA_CATALOG"] || DEFAULT_CATALOG_PATH
      File.expand_path(path)
    end

    def catalog_updated?
      File.mtime(catalog_path) > @catalog_mtime
    end

    def load_catalog!
      loader = CatalogLoader.new(@catalog_path)
      @catalog = loader.load
      @catalog_mtime = File.mtime(@catalog_path)
      $log.info "catalog loaded", path: @catalog_path, mtime: @catalog_mtime
    end
  end
end
