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

require "droonga/loggable"
require "droonga/deferrable"
require "droonga/process_control_protocol"
require "droonga/line_buffer"

module Droonga
  class ProcessSupervisor
    include Loggable
    include Deferrable
    include ProcessControlProtocol

    attr_writer :on_finish

    def initialize(loop, input, output)
      @loop = loop
      @input = create_input(input)
      @output = create_output(output)
    end

    def start
      @loop.attach(@input)
      # logger.trace("start: new input watcher attached",
      #              :watcher => @input)
      @loop.attach(@output)
      # logger.trace("start: new output watcher attached",
      #              :watcher => @output)
    end

    def stop
      @input.close
      # logger.trace("start: input watcher detached",
      #              :watcher => @input)
      @output.close
      # logger.trace("start: output watcher detached",
      #              :watcher => @output)
    end

    def stop_gracefully
      logger.trace("stop_gracefully: start")
      @output.write(Messages::STOP_GRACEFUL)
      logger.trace("stop_gracefully: done")
    end

    def stop_immediately
      logger.trace("stop_immediately: start")
      @output.write(Messages::STOP_IMMEDIATELY)
      logger.trace("stop_immediately: done")
    end

    private
    def create_input(raw_input)
      input = Coolio::IO.new(raw_input)
      line_buffer = LineBuffer.new
      on_read = lambda do |data|
        line_buffer.feed(data) do |line|
          case line
          when Messages::READY
            on_ready
          when Messages::FINISH
            on_finish
          end
        end
      end
      input.on_read do |data|
        on_read.call(data)
      end
      input
    end

    def create_output(raw_output)
      Coolio::IO.new(raw_output)
    end

    def on_finish
      @on_finish.call if @on_finish
    end

    def log_tag
      "process_supervisor"
    end
  end
end
