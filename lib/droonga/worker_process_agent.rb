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

require "coolio"

require "droonga/process_control_protocol"
require "droonga/line_buffer"
require "droonga/loggable"

module Droonga
  class WorkerProcessAgent
    include ProcessControlProtocol
    include Loggable

    def initialize(loop, input, output)
      @loop = loop
      create_input(input)
      create_output(output)
    end

    def start
      logger.trace("start: start")
      @loop.attach(@input)
      # logger.trace("start: new input watcher attached",
      #              :watcher => @input)
      @loop.attach(@output)
      # logger.trace("start: new output watcher attached",
      #              :watcher => @output)
      logger.trace("start: done")
    end

    def stop
      logger.trace("stop: start")
      if @output
        @output, output = nil, @output
        output.write(Messages::FINISH)
        output.on_write_complete do
          output.close
          # logger.trace("stop: output watcher detached",
          #              :watcher => output)
        end
      end
      if @input
        @input, input = nil, @input
        input.close
        # logger.trace("stop: input watcher detached",
        #              :watcher => input)
      end
      logger.trace("stop: done")
    end

    def ready
      @output.write(Messages::READY)
    end

    def on_stop_gracefully=(callback)
      @on_stop_gracefully = callback
    end

    def on_stop_immediately=(callback)
      @on_stop_immediately = callback
    end

    def on_refresh_self_reference=(callback)
      @on_refresh_self_reference = callback
    end

    private
    def create_input(raw_input)
      @input = Coolio::IO.new(raw_input)
      on_read = lambda do |data|
        line_buffer = LineBuffer.new
        line_buffer.feed(data) do |line|
          logger.trace("line buffer feeded", :line => line);
          case line
          when Messages::STOP_GRACEFUL
            on_stop_gracefully
          when Messages::STOP_IMMEDIATELY
            on_stop_immediately
          when Messages::REFRESH_SELF_REFERENCE
            on_refresh_self_reference
          end
        end
      end
      @input.on_read do |data|
        on_read.call(data)
      end
      on_close = lambda do
        if @input
          @input = nil
          on_stop_immediately
        end
      end
      @input.on_close do
        on_close.call
      end
    end

    def create_output(raw_output)
      @output = Coolio::IO.new(raw_output)
      on_close = lambda do
        if @output
          @output = nil
          on_stop_immediately
        end
      end
      @output.on_close do
        on_close.call
      end
    end

    def on_stop_gracefully
      @on_stop_gracefully.call if @on_stop_gracefully
    end

    def on_stop_immediately
      @on_stop_immediately.call if @on_stop_immediately
    end

    def on_refresh_self_reference
      @on_refresh_self_reference.call if @on_refresh_self_reference
    end

    def log_tag
      "worker_process_agent"
    end
  end
end
