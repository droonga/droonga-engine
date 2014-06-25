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
    attr_accessor :on_finish
    def initialize(loop, name, internal_name)
      @loop = loop
      @name = name
      @internal_name = internal_name
      @sessions = {}
      @current_id = 0
      @forwarder = Forwarder.new(@loop)
      @forwarder.resume
      @replier = Replier.new(@forwarder)
      @on_finish = nil
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
    end

    def unregister_session(id)
      @sessions.delete(id)
      unless have_session?
        @on_finish.call if @on_finish
      end
    end

    def have_session?
      not @sessions.empty?
    end

    private
    def log_tag
      "engine_state"
    end
  end
end
