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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "droonga/plugin"

module Droonga
  module Plugins
    module System
      extend Plugin
      register("system")

      class StatusHandler < Droonga::Handler
        action.synchronous = true

        def handle(message)
          cluster = @messenger.engine_state.cluster
          active_nodes = cluster.forwardable_nodes
          dead_nodes = cluster.dead_nodes
          nodes = {}
          cluster.all_nodes.collect do |identifier|
            if active_nodes.include?(identifier)
              status = "active"
            elsif dead_nodes.include?(identifier)
              status = "dead"
            else
              status = "inactive"
            end
            nodes[identifier] = {
              "status" => status,
            }
          end

          {
            "nodes" => nodes,
          }
        end
      end

      define_single_step do |step|
        step.name = "system.status"
        step.handler = StatusHandler
        step.collector = Collectors::Or
      end
    end
  end
end
