# Copyright (C) 2014 Droonga Project
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
require "droonga/reducer"

module Droonga
  module Plugins
    module Basic
      extend Plugin
      register("basic")

      class GatherCollector < Droonga::Collector
        message.pattern = ["task.step.type", :equal, "gather"]

        def collect(message)
          output = message.input || message.name
          if output.is_a?(Hash)
            output_name = output["output"]
          else
            output_name = output
          end
          message.values[output_name] = message.value
        end
      end

      class ReduceCollector < Droonga::Collector
        message.pattern = ["task.step.type", :equal, "reduce"]

        def collect(message)
          message.input.each do |output_name, deal|
            left_value = message.values[output_name]
            right_value = message.value
            reducer = Reducer.new(deal)
            value = reducer.reduce(left_value, right_value)
            message.values[output_name] = value
          end
        end
      end
    end
  end
end
