# Copyright (C) 2015 Droonga Project
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
require "droonga/database_scanner"

module Droonga
  module Plugins
    module System
      class StatisticsObjectCountHandler < Droonga::Handler
        include DatabaseScanner

        def handle(message)
          counts = {}
          output = message.request["output"]
          if output and output.is_a?(Array)
            if output.include?("tables")
              counts["tables"] = n_tables
            end
            if output.include?("columns")
              counts["columns"] = n_columns
            end
            if output.include?("records")
              counts["records"] = n_records
            end
            if output.include?("total")
              counts["total"] = total_n_objects
            end
          end
          counts
        end
      end

      define_single_step do |step|
        step.name = "system.statistics.object.count"
        step.handler = StatisticsObjectCountHandler
        step.collector = Collectors::RecursiveSum
      end
    end
  end
end
