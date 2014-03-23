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
      @server = Coolio::UNIXServer.new(@socket_path) do |connection|
        @job_queue.add_worker(WorkerConnection.new(@loop, connection))
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
      @job_queue.close
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
        @workers = []
      end

      def close
        @workers.each do |worker|
          worker.close
        end
      end

      def add_worker(worker)
        @workers << worker
        worker.on_ready = lambda do |w|
          if @buffers.empty?
            @ready_workers << w
          else
            w.write(@buffers.shift)
          end
        end
      end

      def push(message)
        job = message.to_msgpack
        if @ready_workers.empty?
          @buffers << job
        else
          worker = @ready_workers.shift
          if @buffers.empty?
            worker.write(job)
          else
            @buffers << job
            worker.write(@buffers.shift)
          end
        end
      end
    end

    class WorkerConnection
      attr_writer :on_ready

      def initialize(loop, connection)
        @loop = loop
        @connection = connection
        @ready = false
        @on_ready = nil
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
          @on_ready.call(self) if @on_ready
        end
        @connection.on_read do |data|
          on_read.call(data)
        end
      end
    end
  end
end
