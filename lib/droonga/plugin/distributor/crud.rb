# -*- coding: utf-8 -*-
#
# Copyright (C) 2013-2014 Droonga Project
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
  class CRUDDistributor < Droonga::DistributorPlugin
    repository.register("crud", self)

    command :add
    def add(message)
      scatter_all(message)
    end

    command :update
    def update(message)
      scatter_all(message)
    end

    # TODO: What is this?
    command :reset
    def reset(message)
      scatter_all(message)
    end

    private
    def scatter_all(message)
      planner = DistributedCommandPlanner.new(message)
      planner.key = message["body"]["key"] || rand.to_s
      planner.scatter
      planner.reduce("success", "type" => "and")
      planner.plan
      distribute(planner.messages)
    end
  end
end
