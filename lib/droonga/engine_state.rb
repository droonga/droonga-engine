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
require "droonga/forwarder"
require "droonga/replier"

module Droonga
  class EngineState
    include Loggable

    attr_reader :loop
    attr_reader :name
    attr_reader :internal_name
    attr_reader :forwarder
    attr_reader :replier
    attr_writer :on_ready
    attr_accessor :on_finish
    attr_accessor :catalog
    def initialize(loop, name, internal_name)
      @loop = loop
      @name = name
      @internal_name = internal_name
      @sessions = {}
      @current_id = 0
      @forwarder = Forwarder.new(@loop, :buffering => true)
      @replier = Replier.new(@forwarder)
      @on_ready = nil
      @on_finish = nil
      @catalog = nil
      @live_nodes_list = nil
      @dead_nodes = []
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

    def all_nodes
      @catalog.all_nodes
    end

    # nodes who provide the service actually
    #  * read-only messages  : deliver
    #  * read-write messages : deliver
    #  * responses           : returned
    def active_nodes
      live_nodes - suspended_nodes
    end

    # nodes in the serf cluster
    #  * read-only messages  : deliver
    #  * read-write messages : deliver
    #  * responses           : undetermined
    def live_nodes
      if @live_nodes_list
        @live_nodes_list.live_nodes
      else
        all_nodes
      end
    end

    # nodes who temporary suspended
    # (going to join to the cluster, etc,)
    #  * read-only messages  : don't deliver
    #  * read-write messages : deliver
    #  * responses           : not returned
    def suspended_nodes
      if @live_nodes_list
        @live_nodes_list.suspended_nodes
      else
        []
      end
    end

    def live_nodes_list=(nodes_list)
      old_live_nodes_list = @live_nodes_list
      @live_nodes_list = nodes_list
      @dead_nodes = all_nodes - @live_nodes_list.live_nodes
      if old_live_nodes_list != @live_nodes_list.live_nodes
        @forwarder.resume
      end
      @live_nodes_list
    end

    def remove_dead_routes(routes)
      routes.reject do |route|
        @dead_nodes.include?(farm_path(route))
      end
    end

    def on_ready
      @on_ready.call if @on_ready
    end

    private
    def log_tag
      "engine_state"
    end
  end
end
