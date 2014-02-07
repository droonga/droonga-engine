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

require "droonga/legacy_plugin"
require "droonga/distributed_command_planner"

module Droonga
  class PlannerPlugin < LegacyPlugin
    extend PluginRegisterable

    def initialize(planner)
      super()
      @planner = planner
    end

    def distribute(messages)
      @planner.distribute(messages)
    end

    def scatter(message, options={})
      planner = DistributedCommandPlanner.new(message)
      planner.scatter
      planner.key = options[:key]
      planner.reduce(options[:reduce])
      distribute(planner.plan)
    end

    def broadcast(message, options={})
      planner = DistributedCommandPlanner.new(message)
      planner.broadcast(:write => options[:write])
      planner.reduce(options[:reduce])
      distribute(planner.plan)
    end

    private
    def process_error(command, error, arguments)
      if error.is_a?(MessageProcessingError)
        raise error
      else
        super
      end
    end
  end
end
