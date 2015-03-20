# Copyright (C) 2013-2015 Droonga Project
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

require "msgpack"

require "droonga/logger"
require "droonga/job_protocol"

module Droonga
  class JobPusher
    include Loggable

    attr_reader :socket_path
    def initialize(loop, base_path)
      @loop = loop
      @socket_path = "#{base_path}.#{Process.pid}.#{object_id}.sock"
      @job_queue = JobQueue.new(@loop)
      @server = nil
    end

    def start
      FileUtils.rm_f(@socket_path)
      @server = Coolio::UNIXServer.new(@socket_path) do |connection|
        @job_queue.add_worker(WorkerConnection.new(connection))
      end
      FileUtils.chmod(0600, @socket_path)
      @loop.attach(@server)
    end

    def close
      @server.close if @server
    end

    def shutdown
      logger.trace("shutdown: start")
      @server.close if @server
      @job_queue.close
      FileUtils.rm_f(@socket_path)
      logger.trace("shutdown: done")
    end

    def push(message)
      logger.trace("push: start")
      @job_queue.push(message)
      logger.trace("push: done")
    end

    def broadcast(message)
      logger.trace("broadcast start")
      @job_queue.broadcast(message)
      logger.trace("broadcast done")
    end

    private
    def log_tag
      "job_pusher"
    end

    class JobQueue
      include Loggable

      def initialize(loop)
        @loop = loop
        @buffers = []
        @ready_workers = []
        @workers = []
        @many_jobs_report_interval = 100
        update_many_jobs_threshold
      end

      def close
        @workers.each do |worker|
          worker.close
        end
      end

      def add_worker(worker)
        @workers << worker
        update_many_jobs_threshold
        worker.on_ready = lambda do |ready_worker|
          supply_job(ready_worker)
        end
      end

      def push(message)
        job = message.to_msgpack
        if @ready_workers.empty?
          @buffers << job
          report_statistics_on_push
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

      def broadcast(message)
        @workers.each do |worker|
          worker.write(message.to_msgpack)
        end
      end

      private
      def supply_job(worker)
        if @buffers.empty?
          @ready_workers << worker
        else
          worker.write(@buffers.shift)
          report_statistics_on_pull
        end
      end

      def update_many_jobs_threshold
        @many_jobs_threshold = @workers.size * 100
      end

      def report_statistics_on_push
        if @buffers.size >= @many_jobs_threshold
          if (@buffers.size % @many_jobs_report_interval).zero?
            logger.warn("push: many jobs in queue: #{@buffers.size}")
          end
        end
      end

      def report_statistics_on_pull
        if @buffers.size >= @many_jobs_threshold
          if (@buffers.size % @many_jobs_report_interval).zero?
            logger.info("pull: many jobs in queue: #{@buffers.size}")
          end
        elsif @buffers.size == (@many_jobs_threshold - 1)
          logger.info("pull: reducing jobs in queue: #{@buffers.size}")
        end
      end

      def log_tag
        "job_queue"
      end
    end

    class WorkerConnection
      attr_writer :on_ready

      def initialize(connection)
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
