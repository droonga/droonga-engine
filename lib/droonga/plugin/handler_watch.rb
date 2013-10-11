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
  class WatchHandler < Droonga::Handler
    Droonga::HandlerPlugin.register("watch", self)

    command "watch"
    def watch(request)
      p parse_request(request)
      # TODO
    end

    private
    def parse_request(request)
      user = request["user"]
      condition = request["condition"]
      route = request["route"]
      raise "invalid request" if user.nil? || user.empty? || condition.nil?
      query = condition.to_json
      raise "too long query" if query.size > 4095
      [user, condition, query, route]
    end
  end
end
