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

require "coolio"

require "droonga/process_control_protocol"
require "droonga/line_buffer"

module Droonga
  class WorkerProcessAgent
    include ProcessControlProtocol

    def initialize(loop, input, output)
      @loop = loop
      create_input(input)
      create_output(output)
      @on_ready = nil
      @on_finish = nil
    end

    def start
      @loop.attach(@input)
      @loop.attach(@output)
    end

    def stop
      if @output
        @output, output = nil, @output
        output.write(Messages::FINISH)
        output.close
      end
      if @input
        @input, input = nil, @input
        input.close
      end
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

    private
    def create_input(raw_input)
      @input = Coolio::IO.new(raw_input)
      on_read = lambda do |data|
        line_buffer = LineBuffer.new
        line_buffer.feed(data) do |line|
          case line
          when Messages::STOP_GRACEFUL
            on_stop_gracefully
          when Messages::STOP_IMMEDIATELY
            on_stop_immediately
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
  end
end
