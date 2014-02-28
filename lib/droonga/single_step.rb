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

require "droonga/planner"
require "droonga/collectors"

module Droonga
  class SingleStep
    def initialize(definition)
      @definition = definition
    end

    def plan(message)
      if message["type"] == "search"
        # XXX: workaround
        planner = Plugins::Search::Planner.new
        return planner.plan(message)
      end

      # XXX: Re-implement me.
      planner = Planner.new
      options = {}
      options[:write] = @definition.write?
      collector_class = @definition.collector_class
      if collector_class
        reduce_key = "result"
        options[:reduce] = {
          reduce_key => collector_class.operator,
        }
      end
      inputs = @definition.inputs
      if inputs.empty?
        planner.send(:broadcast, message, options)
      else
        input = inputs.values.first
        options[:key] = message["body"][input[:filter]]["key"]
        planner.send(:scatter, message, options)
      end
    end
  end
end
