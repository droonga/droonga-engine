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
  class DistributorPlugin < Plugin
    extend PluginRegisterable

    def initialize(distributor)
      super()
      @distributor = distributor
    end

    # TODO: consider better name
    def post(message)
      @distributor.post(message)
    end

    def scatter_all(envelope, key)
      message = [{
        "command"=> envelope["type"],
        "dataset"=> envelope["dataset"],
        "body"=> envelope["body"],
        "key"=> key,
        "type"=> "scatter",
        "replica"=> "all",
        "post"=> true
      }]
      post(message)
    end

    def broadcast_all(envelope)
      distribute_message = [{
        "command"=> envelope["type"],
        "dataset"=> envelope["dataset"],
        "body"=> envelope["body"],
        "type"=> "broadcast",
        "replica"=> "all",
        "post"=> true
      }]
      post(distribute_message)
    end
  end
end
