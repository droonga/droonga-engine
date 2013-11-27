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

require "droonga/handler"

module Droonga
  module Worker
    def initialize
      @handler = Handler.new(config)
    end

    def run
      $log.trace("#{log_tag}: run: start")
      @running = true
      while @running
        $log.trace("#{log_tag}: run: pull_message: start")
        @handler.execute_one
        $log.trace("#{log_tag}: run: pull_message: done")
      end
      @handler.shutdown
      $log.trace("#{log_tag}: run: done")
    end

    def stop
      $log.trace("#{log_tag}: stop: start")
      @running = false
      $log.trace("#{log_tag}: stop: done")
    end

    private
    def log_tag
      "[#{Process.ppid}][#{Process.pid}] worker"
    end
  end
end
