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

require "droonga/executor"

module Droonga
  class Engine
    def initialize(options={})
      @options = options
    end

    def start
      @executor = Executor.new(@options)
    end

    def shutdown
      $log.trace("engine: shutdown: start")
      @executor.shutdown if @executor
      $log.trace("engine: shutdown: done")
    end

    def emit(tag, time, record, synchronous=nil)
      $log.trace("[#{Process.pid}] tag: <#{tag}> caller: <#{caller.first}>")
      @executor.dispatch(tag, time, record, synchronous)
    end
  end
end
