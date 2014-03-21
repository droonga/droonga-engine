# Copyright (C) 2013-2014 Droonga Project
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

require "msgpack"

require "droonga/logger"
require "droonga/job_protocol"

module Droonga
  class JobPusher
    include Loggable

    attr_reader :socket_path
    def initialize(loop, base_path)
      @loop = loop
      @socket_path = "#{base_path}.sock"
      @job_queue = JobQueue.new(@loop)
    end

    def start
      FileUtils.rm_f(@socket_path)
      @workers = []
      @server = Coolio::UNIXServer.new(@socket_path) do |connection|
        @workers << WorkerConnection.new(@loop, @job_queue, connection)
      end
      FileUtils.chmod(0600, @socket_path)
      @loop.attach(@server)
    end

    def close
      @server.close
    end

    def shutdown
      logger.trace("shutdown: start")
      @server.close
      @workers.each do |worker|
        worker.close
      end
      FileUtils.rm_f(@socket_path)
      logger.trace("shutdown: done")
    end

    def push(message)
      logger.trace("push: start")
      @job_queue.push(message)
      logger.trace("push: done")
    end

    private
    def log_tag
      "job_pusher"
    end

    class JobQueue
      def initialize(loop)
        @loop = loop
        @buffers = []
        @ready_workers = []
      end

      def push(message)
        job = message.to_msgpack
        @buffers << job
        consume_buffers
      end

      def ready(worker)
        if @buffers.empty?
          @ready_workers << worker
        else
          worker.write(@buffers.shift)
        end
      end

      private
      def consume_buffers
        return if @ready_workers.empty?
        until @buffers.empty?
          while worker = @ready_workers.shift
            worker.write(@buffers.shift)
            return if @buffers.empty?
          end
        end
      end
    end

    class WorkerConnection
      def initialize(loop, job_queue, connection)
        @loop = loop
        @job_queue = job_queue
        @connection = connection
        @ready = false
        setup_connection
      end

      def ready?
        @ready
      end

      def write(job)
        @connection.write(job)
        @ready = false
        @loop.break_current_loop
      end

      def close
        @connection.close
      end

      private
      def setup_connection
        on_read = lambda do |data|
          @ready = (data == JobProtocol::READY_SIGNAL)
          @job_queue.ready(self)
        end
        @connection.on_read do |data|
          on_read.call(data)
        end
      end
    end
  end
end
