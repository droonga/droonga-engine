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

require "coolio"

require "droonga/path"
require "droonga/loggable"

module Droonga
  class FileObserver
    include Loggable

    CHECK_INTERVAL = 1

    attr_accessor :on_change

    def initialize(loop, path)
      @loop = loop
      @path = path
      if @path.exist?
        @mtime = @path.mtime
      else
        @mtime = nil
      end
      @on_change = nil
    end

    def start
      @watcher = Cool.io::TimerWatcher.new(CHECK_INTERVAL, true)
      on_timer = lambda do
        if updated?
          @mtime = @path.mtime
          @on_change.call if @on_change
        end
      end
      @watcher.on_timer do
        on_timer.call
      end
      @loop.attach(@watcher)
    end

    def stop
      @watcher.detach
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
