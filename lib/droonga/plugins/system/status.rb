# Copyright (C) 2013-2015 Droonga Project
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
      class StatusHandler < Droonga::Handler
        action.synchronous = true

        def handle(message)
          {
            "nodes"    => cluster.engine_nodes_status,
            "reporter" => reporter,
          }
        end

        private
        def cluster
          @messenger.cluster
        end

        def reporter
          "#{engine_state.internal_name} @ #{engine_state.name}"
        end

        def engine_state
          @messenger.engine_state
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
