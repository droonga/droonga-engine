# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2015 Droonga Project
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

require "coolio"

require "droonga/path"
require "droonga/loggable"
require "droonga/changable"

module Droonga
  class FileObserver
    include Loggable
    include Changable

    CHECK_INTERVAL = 1

    def initialize(loop, path)
      @loop = loop
      @path = path
      if @path.exist?
        @mtime = @path.mtime
      else
        @mtime = nil
      end
    end

    def start
      @timer = Coolio::TimerWatcher.new(CHECK_INTERVAL, true)
      @timer.on_timer do
        if updated?
          @mtime = @path.mtime
          on_change
        end
      end
      @loop.attach(@timer)
      logger.trace("start: timer attached",
                   :watcher => @timer,
                   :path    => @path)
    end

    def stop
      @timer.detach if @timer
      logger.trace("stop: timer detached",
                   :watcher => @timer,
                   :path    => @path)
      @timer = nil
    end

    private
    def updated?
      return false unless @path.exist?
      @mtime.nil? or @path.mtime > @mtime
    end

    def log_tag
      "file-observer"
    end
  end
end
