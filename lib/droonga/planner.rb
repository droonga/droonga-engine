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

    attr_writer :write, :single_operation, :collector_class

    def initialize(dataset)
      @dataset = dataset
      @write = false
      @single_operation = false
      @collector_class = nil
    end

    def plan(message, params={})
      options = {
        :record => params[:record],
      }
      if @collector_class
        reduce_key = "result"
        options[:reduce] = {
          reduce_key => @collector_class.operator,
        }
      end

      if options[:record] or single_operation?
        scatter(message, options)
      else
        broadcast(message, options)
      end
    end

    private
    def write?
      @write
    end

    def single_operation?
      return false if write?
      @single_operation
    end

    def scatter(message, options={})
      planner = DistributedCommandPlanner.new(@dataset, message)
      scatter_options = {
        :write => write?,
        :record => options[:record],
      }
      if single_operation?
        scatter_options[:slice]   = "random"
        scatter_options[:replica] = "random"
      end
      planner.scatter(scatter_options)
      planner.reduce(options[:reduce])
      planner.plan
    end

    def broadcast(message, options={})
      planner = DistributedCommandPlanner.new(@dataset, message)
      broadcast_options = {
        :write => write?,
      }
      if write?
        broadcast_options[:replica] = "all"
      elsif single_operation?
        broadcast_options[:slice]   = "random"
        broadcast_options[:replica] = "random"
      end
      planner.broadcast(broadcast_options)
      planner.reduce(options[:reduce])
      planner.plan
    end
  end
end
