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
require "droonga/node_status"

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
      @forwarder = BufferedForwarder.new(@loop,
                                 :buffering => true,
                                 :engine_state => self)
      @replier = Replier.new(@forwarder)
      @on_ready = nil
      @on_finish = nil
      @catalog = nil
      @live_nodes_list = nil
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

    def unwritable_node?(node_name)
      case node_status.role
      when NodeStatus::Role::SERVICE_PROVIDER
        absorb_source_nodes.include?(node_name) or
          absorb_destination_nodes.include?(node_name)
      when NodeStatus::Role::ABSORB_SOURCE
        absorb_destination_nodes.include?(node_name)
      else
        false
      end
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

    def dead_nodes
      if @live_nodes_list
        @live_nodes_list.dead_nodes
      else
        []
      end
    end

    def service_provider_nodes
      if @live_nodes_list
        @live_nodes_list.service_provider_nodes
      else
        all_nodes
      end
    end

    def absorb_source_nodes
      if @live_nodes_list
        @live_nodes_list.absorb_source_nodes
      else
        []
      end
    end

    def absorb_destination_nodes
      if @live_nodes_list
        @live_nodes_list.absorb_destination_nodes
      else
        []
      end
    end

    def same_role_nodes
      case node_status.role
      when NodeStatus::Role::SERVICE_PROVIDER
        all_nodes & service_provider_nodes
      when NodeStatus::Role::ABSORB_SOURCE
        all_nodes & absorb_source_nodes
      when NodeStatus::Role::ABSORB_DESTINATION
        all_nodes & absorb_destination_nodes
      else
        []
      end
    end

    def forwardable_nodes
      same_role_nodes - dead_nodes
    end

    def writable_nodes
      case node_status.role
      when NodeStatus::Role::SERVICE_PROVIDER
        all_nodes
      when NodeStatus::Role::ABSORB_SOURCE
        all_nodes & absorb_source_nodes
      when NodeStatus::Role::ABSORB_DESTINATION
        all_nodes & absorb_destination_nodes
      else
        []
      end
    end

    def live_nodes_list=(new_nodes_list)
      old_live_nodes_list = @live_nodes_list
      @live_nodes_list = new_nodes_list
      unless old_live_nodes_list == new_nodes_list
        @forwarder.resume
      end
      @live_nodes_list
    end

    def select_responsive_routes(routes)
      selected_nodes = forwardable_nodes
      routes.select do |route|
        selected_nodes.include?(farm_path(route))
      end
    end

    def on_ready
      @on_ready.call if @on_ready
    end

    private
    def node_status
      @node_status ||= NodeStatus.new
    end

    def log_tag
      "engine_state"
    end
  end
end
