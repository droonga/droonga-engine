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

require "droonga/distributor_plugin"

module Droonga
  class WatchDistributor < Droonga::DistributorPlugin
    repository.register("watch", self)

    command "watch.feed" => :feed
    def feed(message)
      broadcast_all(message)
    end

    command "watch.subscribe" => :subscribe
    def subscribe(message)
      broadcast_all(message)
    end

    command "watch.unsubscribe" => :unsubscribe
    def unsubscribe(message)
      broadcast_all(message)
    end

    command "watch.sweep" => :sweep
    def sweep(message)
      broadcast_all(message)
    end

    private
    def broadcast_all(message)
      planner = DistributedCommandPlanner.new(message)
      planner.broadcast(:write => true)
      planner.reduce("success", "type" => "or")
      planner.plan
      distribute(planner.messages)
    end
  end
end
