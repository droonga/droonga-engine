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

require "droonga/loggable"
require "droonga/distributed_command_planner"
require "droonga/error_messages"

module Droonga
  class Planner
    include Loggable
    include ErrorMessages

    def initialize(dataset)
      @dataset = dataset
    end

    def plan(message)
      raise NotImplemented, "#{self.class.name}\##{__method__} must implement."
    end

    private
    def scatter(message, record, options={})
      planner = DistributedCommandPlanner.new(@dataset, message)
      scatter_options = {
        :write => options[:write],
      }
      planner.scatter(record, scatter_options)
      planner.reduce(options[:reduce])
      planner.plan
    end

    def broadcast(message, options={})
      planner = DistributedCommandPlanner.new(@dataset, message)
      broadcast_options = {
        :write => options[:write],
      }
      broadcast_options[:replica] = "all" if options[:write]
      planner.broadcast(broadcast_options)
      planner.reduce(options[:reduce])
      planner.plan
    end
  end
end
