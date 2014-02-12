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

require "droonga/logger"
require "droonga/dispatcher"

module Droonga
  class Engine
    def initialize(options={})
      @options = options
      @dispatcher = Dispatcher.new(@options)
    rescue StartupError => error
      $log.error(error.message)
      exit(1)
    end

    def start
      @dispatcher.start
    end

    def shutdown
      $log.trace("engine: shutdown: start")
      @dispatcher.shutdown
      $log.trace("engine: shutdown: done")
    end

    def process(message)
      @dispatcher.process_message(message)
    end
  end
end
