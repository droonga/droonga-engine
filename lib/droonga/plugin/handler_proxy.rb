# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 droonga project
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
require "droonga/proxy"

module Droonga
  class ProxyHandler < Droonga::Handler
    Droonga::HandlerPlugin.register("proxy", self)

    def initialize(*arguments)
      super
      @proxy = Droonga::Proxy.new(@worker, @worker.name)
    end

    command :proxy

    def proxy(request, *arguments)
      @proxy.handle(request)
    end
  end
end
