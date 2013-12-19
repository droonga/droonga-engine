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

require "droonga/plugin"

module Droonga
  class HandlerPlugin < Plugin
    extend PluginRegisterable

    def initialize(handler)
      super()
      @handler = handler
      @context = @handler.context
    end

    def envelope
      @handler.envelope
    end

    def emit(value)
      @handler.emit(value)
    end

    def forward(body, destination)
      @handler.forward(body, destination)
    end

    def prefer_synchronous?(command)
      false
    end
  end
end
