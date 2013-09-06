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

module Droonga
  class Adapter < Droonga::Handler
    Droonga::HandlerPlugin.register("adapter", self)

    command :table_create
    def table_create(request)
      broadcast_all(request)
    end

    command :column_create
    def column_create(request)
      broadcast_all(request)
    end

    def broadcast_all(request)
      message = [{
        "command"=> envelope["type"],
        "dataset"=> envelope["dataset"],
        "body"=> request,
        "type"=> "broadcast",
        "replica"=> "all",
        "post"=> true
      }]
      post(message, "proxy")
    end
  end
end
