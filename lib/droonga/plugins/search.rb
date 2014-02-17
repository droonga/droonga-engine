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

require "droonga/plugin"
require "droonga/searcher"
require "droonga/plugins/search/distributed_search_planner"

module Droonga
  module Plugins
    module Search
      Plugin.registry.register("search", self)

      class Planner < Droonga::Planner
        message.pattern = ["type", :equal, "search"]

        def plan(message)
          planner = DistributedSearchPlanner.new(message)
          planner.plan
        end
      end

      class Handler < Droonga::Handler
        message.type = "search"

        def handle(message, messenger)
          searcher = Droonga::Searcher.new(@context)
          values = {}
          request = message.request
          raise Droonga::Searcher::NoQuery.new unless request
          searcher.search(request["queries"]).each do |output, value|
            values[output] = value
          end
          messenger.emit(values)
        end
      end
    end
  end
end
