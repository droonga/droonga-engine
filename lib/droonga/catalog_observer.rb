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

require "droonga/loggable"
require "droonga/catalog_loader"

module Droonga
  class CatalogObserver
    include Loggable

    DEFAULT_CATALOG_PATH = "catalog.json"
    CHECK_INTERVAL = 1

    attr_reader :catalog
    attr_accessor :on_reload

    def initialize(loop)
      @loop = loop
      @catalog_path = catalog_path
      load_catalog!
    end

    def start
      @watcher = Cool.io::TimerWatcher.new(CHECK_INTERVAL, true)
      observer = self
      @watcher.on_timer do
        observer.ensure_latest_catalog_loaded
      end
      @loop.attach(@watcher)
    end

    def stop
      @watcher.detach
    end

    def ensure_latest_catalog_loaded
      if catalog_updated?
        begin
          load_catalog!
          on_reload.call(catalog) if on_reload
        rescue Droonga::Error => error
          logger.warn("reload: fail", :path => @catalog_path, :error => error)
        end
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
      logger.info("loaded", :path => @catalog_path, :mtime => @catalog_mtime)
    ensure
      @catalog_mtime = File.mtime(@catalog_path)
    end

    private
    def log_tag
      "catalog-observer"
    end
  end
end
