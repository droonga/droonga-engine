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

require "coolio"

require "droonga/loggable"
require "droonga/deferrable"

module Droonga
  class Serf
    class Agent
      # the port must be different from droonga-http-server's agent!
      PORT = 7946

      include Loggable
      include Deferrable

      MAX_N_READ_CHECKS = 10

      def initialize(loop, serf, host, bind_port, rpc_port, *options)
        @loop = loop
        @serf = serf
        @host = host
        @bind_port = bind_port
        @rpc_port = rpc_port
        @options = options
        @pid = nil
        @n_ready_checks = 0
      end

      def start
        capture_output do |output_write, error_write|
          env = {}
          spawn_options = {
            :out => output_write,
            :err => error_write,
          }
          @pid = spawn(env, @serf, "agent",
                       "-bind", "#{@host}:#{@bind_port}",
                       "-rpc-addr", "#{@host}:#{@rpc_port}",
                       "-log-level", serf_log_level,
                       *@options, spawn_options)
        end
        start_ready_check
      end

      def stop
        return if @pid.nil?
        Process.waitpid(@pid)
        @output_io.close
        # logger.trace("stop: output_io watcher detached",
        #              :watcher => @output_io)
        @error_io.close
        # logger.trace("stop: error_io watcher detached",
        #              :watcher => @error_io)
        @pid = nil
      end

      def running?
        not @pid.nil?
      end

      private
      def serf_log_level
        level = Logger::Level.default
        case level
        when "trace", "debug", "info", "warn"
          level
        when "error", "fatal"
          "err"
        else
          level # Or error?
        end
      end

      def capture_output
        result = nil
        output_read, output_write = IO.pipe
        error_read, error_write = IO.pipe

        begin
          result = yield(output_write, error_write)
        rescue
          output_read.close  unless output_read.closed?
          output_write.close unless output_write.closed?
          error_read.close   unless error_read.closed?
          error_write.close  unless error_write.closed?
          raise
        end

        output_line_buffer = LineBuffer.new
        on_read_output = lambda do |data|
          on_standard_output(output_line_buffer, data)
        end
        @output_io = Coolio::IO.new(output_read)
        @output_io.on_read do |data|
          on_read_output.call(data)
        end
        @loop.attach(@output_io)
        # logger.trace("capture_output: new output_io watcher attached",
        #              :watcher => @output_io)

        error_line_buffer = LineBuffer.new
        on_read_error = lambda do |data|
          on_error_output(error_line_buffer, data)
        end
        @error_io = Coolio::IO.new(error_read)
        @error_io.on_read do |data|
          on_read_error.call(data)
        end
        @loop.attach(@error_io)
        # logger.trace("capture_output: new error_io watcher attached",
        #              :watcher => @error_io)

        result
      end

      def on_standard_output(line_buffer, data)
        line_buffer.feed(data) do |line|
          line = line.chomp
          case line
          when /\A==> /
            content = $POSTMATCH
            logger.info(content)
          when /\A    /
            content = $POSTMATCH
            case content
            when /\A(\d{4})\/(\d{2})\/(\d{2}) (\d{2}):(\d{2}):(\d{2}) \[(\w+)\] /
              # year, month, day = $1, $2, $3
              # hour, minute, second = $4, $5, $6
              level = $7
              content = $POSTMATCH
              return unless needed_log_message?(content)
              level = normalize_level(level)
              logger.send(level, content)
            else
              logger.info(content)
            end
          else
            logger.info(line)
          end
        end
      end

      def needed_log_message?(content)
        case content
        when /\Amemberlist: Failed to receive remote state: EOF\z/
          # See also: https://github.com/hashicorp/consul/issues/598#issuecomment-71576948
          false
        when /\Aagent: Script .*droonga-engine-serf-event-handler.* slow, execution exceeding/
          # Droonga's serf event handler can be slow for absorbing or some operations.
          false
        else
          true
        end
      end

      def normalize_level(level)
        level = level.downcase
        case level
        when "err"
          "error"
        else
          level
        end
      end

      def on_error_output(line_buffer, data)
        line_buffer.feed(data) do |line|
          line = line.chomp
          logger.error(line.gsub(/\A==> /, ""))
        end
      end

      def start_ready_check
        @n_ready_checks += 1

        checker = Coolio::TCPSocket.connect(@host, @bind_port)

        on_connect = lambda do
          on_ready
          checker.close
          # logger.trace("start_ready_check: checker watcher detached",
          #              :watcher => checker)
        end
        checker.on_connect do
          on_connect.call
        end

        on_connect_failed = lambda do
          if @n_ready_checks >= MAX_N_READ_CHECKS
            on_failure
          else
            timer = Coolio::TimerWatcher.new(1)
            on_timer = lambda do
              start_ready_check
              timer.detach
              # logger.trace("start_ready_check: timer watcher detached",
              #              :watcher => timer)
            end
            timer.on_timer do
              on_timer.call
            end
            @loop.attach(timer)
            # logger.trace("start_ready_check: new timer watcher attached",
            #              :watcher => timer)
          end
        end
        checker.on_connect_failed do
          on_connect_failed.call
        end

        @loop.attach(checker)
        # logger.trace("start_ready_check: new checker watcher attached",
        #              :watcher => checker)
      end

      def log_tag
        tag = "serf-agent"
        tag << "[#{@pid}]" if @pid
        tag
      end
    end
  end
end
