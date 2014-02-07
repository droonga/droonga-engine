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

require "droonga/planner_plugin"

module Droonga
  class GroongaPlanner < Droonga::PlannerPlugin
    repository.register("groonga", self)

    command :table_create
    def table_create(message)
      unless message["dataset"]
        raise "dataset must be set. FIXME: This error should return client."
      end
      broadcast(message)
    end

    command :table_remove
    def table_remove(message)
      unless message["dataset"]
        raise "dataset must be set. FIXME: This error should return client."
      end
      broadcast(message)
    end

    command :column_create
    def column_create(message)
      broadcast(message)
    end

    private
    def broadcast(message)
      super(message,
            :write => true,
            :reduce => {
              "result" => "or"
            })
    end
  end
end
