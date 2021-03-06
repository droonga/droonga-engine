# Copyright (C) 2014-2015 Droonga Project
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

require "droonga/loggable"
require "droonga/deferrable"
require "droonga/address"
require "droonga/event_loop"
require "droonga/forwarder"
require "droonga/replier"

module Droonga
  class EngineState
    include Loggable
    include Deferrable

    DEFAULT_SESSION_TIMEOUT_SECONDS = 60

    attr_reader :loop
    attr_reader :name
    attr_reader :internal_name
    attr_reader :internal_connection_lifetime
    attr_reader :forwarder
    attr_reader :replier
    attr_accessor :catalog
    attr_accessor :on_finish

    def initialize(params)
      @loop = params[:loop]
      @name = params[:name]
      @internal_name = params[:internal_name]
      @internal_connection_lifetime = params[:internal_connection_lifetime]
      @sessions = {}
      @current_id = 0
      @forwarder = create_forwarder
      @replier = create_replier
      @on_finish = nil
      @catalog = params[:catalog]
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

    def internal_route(route)
      name = Address.parse(route).node
      if name == @name
        route.sub(name, @internal_name)
      else
        route
      end
    rescue ArgumentError
      route
    end

    def public_route(route)
      name = Address.parse(route).node
      if name == @internal_name
        route.sub(name, @name)
      else
        route
      end
    rescue ArgumentError
      route
    end

    def internal_farm_path(route)
      name = Address.parse(route).node
      if name == @name
        @internal_name
      else
        name
      end
    rescue ArgumentError
      route
    end

    def public_farm_path(route)
      name = Address.parse(route).node
      if name == @internal_name
        @name
      else
        name
      end
    rescue ArgumentError
      route
    end

    def generate_id
      id = @current_id
      @current_id = id.succ
      return [@internal_name, id].join(".#")
    end

    def find_session(id)
      @sessions[id]
    end

    def register_session(id, session, options={})
      @sessions[id] = session
      logger.trace("new session #{id} is registered. rest sessions=#{@sessions.size}")

      timeout = options[:timeout] ||
        @internal_connection_lifetime ||
        DEFAULT_SESSION_TIMEOUT_SECONDS
      session.set_timeout(@loop, timeout) do
        logger.trace("session #{id} is timed out!")
        unregister_session(id)
      end
    end

    def unregister_session(id)
      session = @sessions[id]
      session.finish
      @sessions.delete(id)
      unless have_session?
        @on_finish.call if @on_finish
      end
      logger.trace("session #{id} is unregistered. rest sessions=#{@sessions.size}")
    end

    def have_session?
      not @sessions.empty?
    end

    private
    def create_forwarder
      Forwarder.new(@loop,
                    :auto_close_timeout =>
                      @internal_connection_lifetime)
    end

    def create_replier
      Replier.new(@forwarder)
    end

    def log_tag
      "engine_state"
    end
  end
end
