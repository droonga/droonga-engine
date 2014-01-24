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
  class GroongaDistributor < Droonga::DistributorPlugin
    repository.register("groonga", self)

    command :table_create
    def table_create(message)
      unless message["dataset"]
        raise "dataset must be set. FIXME: This error should return client."
      end
      broadcast_all(message)
    end

    command :table_remove
    def table_remove(message)
      unless message["dataset"]
        raise "dataset must be set. FIXME: This error should return client."
      end
      broadcast_all(message)
    end

    command :column_create
    def column_create(message)
      broadcast_all(message)
    end

    private
    def broadcaster(message)
      broadcaster = super
      broadcaster["outputs"] << "result"
      broadcaster
    end

    def reducer(message)
      reducer = super
      reducer["type"] = "groonga_reduce"
      reducer["body"]["result"] = {
        "result_reduced" => {
          "type" => "groonga_result",
        },
      }
      reducer["inputs"] << "result"
      reducer["outputs"] << "result_reduced"
      reducer
    end

    def gatherer(message)
      gatherer = super
      reducer["type"] = "groonga_gather"
      gatherer["body"]["result_reduced"] = {
        "output" => "result",
      }
      gatherer["inputs"] << "result_reduced"
      gatherer
    end
  end
end
