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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "English"

require "coolio"

require "droonga/loggable"
require "droonga/event_loop"
require "droonga/buffered_forwarder"
require "droonga/replier"
require "droonga/cluster_state"

module Droonga
  class EngineState
    include Loggable

    attr_reader :loop
    attr_reader :name
    attr_reader :internal_name
    attr_reader :forwarder
    attr_reader :replier
    attr_writer :on_ready
    attr_reader :catalog
    attr_reader :cluster
    attr_accessor :on_finish

    def initialize(loop, name, internal_name)
      @loop = loop
      @name = name
      @internal_name = internal_name
      @sessions = {}
      @current_id = 0
      @cluster = ClusterState.new(@loop)
      @forwarder = BufferedForwarder.new(@loop,
                                         :cluster_state => @cluster)
      @cluster.on_change = lambda do
        @forwarder.resume
      end
      @replier = Replier.new(@forwarder)
      @on_ready = nil
      @on_finish = nil
      @catalog = nil
    end

    def catalog=(catalog)
      @catalog = @cluster.catalog = catalog
    end

    def start
      logger.trace("start start")
      @forwarder.start
      logger.trace("start done")
    end

    def shutdown
      logger.trace("shutdown: start")
      @forwarder.shutdown
      logger.trace("shutdown: done")
    end

    def local_route?(route)
      route.start_with?(@name) or route.start_with?(@internal_name)
    end

    def farm_path(route)
      if /\A[^:]+:\d+\/[^.]+/ =~ route
        name = $MATCH
        if name == @internal_name
          @name
        else
          name
        end
      else
        route
      end
    end

    def generate_id
      id = @current_id
      @current_id = id.succ
      return [@internal_name, id].join(".#")
    end

    def find_session(id)
      @sessions[id]
    end

    def register_session(id, session)
      @sessions[id] = session
      logger.trace("new session #{id} is registered. rest sessions=#{@sessions.size}")
    end

    def unregister_session(id)
      @sessions.delete(id)
      unless have_session?
        @on_finish.call if @on_finish
      end
      logger.trace("session #{id} is unregistered. rest sessions=#{@sessions.size}")
    end

    def have_session?
      not @sessions.empty?
    end

    def on_ready
      @on_ready.call if @on_ready
    end

    def select_responsive_routes(routes)
      selected_nodes = @cluster.forwardable_nodes
      routes.select do |route|
        selected_nodes.include?(farm_path(route))
      end
    end

    private
    def log_tag
      "engine_state"
    end
  end
end
