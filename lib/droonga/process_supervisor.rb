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

require "droonga/deferrable"
require "droonga/process_control_protocol"
require "droonga/line_buffer"

module Droonga
  class ProcessSupervisor
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
      @loop.attach(@output)
    end

    def stop
      @input.close
      @output.close
    end

    def stop_gracefully
      @output.write(Messages::STOP_GRACEFUL)
    end

    def stop_immediately
      @output.write(Messages::STOP_IMMEDIATELY)
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
  end
end
