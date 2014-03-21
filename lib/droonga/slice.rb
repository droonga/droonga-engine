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

require "serverengine"

require "droonga/loggable"
require "droonga/server"
require "droonga/worker"
require "droonga/event_loop"
require "droonga/job_pusher"
require "droonga/processor"

module Droonga
  class Slice
    include Loggable

    def initialize(loop, options={})
      @options = options
      @n_workers = @options[:n_workers] || 0
      @loop = loop
      @job_pusher = JobPusher.new(@loop, @options[:database])
      @processor = Processor.new(@loop, @job_pusher, @options)
      @supervisor = nil
    end

    def start
      ensure_database
      @processor.start
      @job_pusher.start
      start_supervisor if @n_workers > 0
    end

    def shutdown
      logger.trace("shutdown: start")
      shutdown_supervisor if @supervisor
      @job_pusher.shutdown
      @processor.shutdown
      logger.trace("shutdown: done")
    end

    def process(message)
      logger.trace("process: start")
      @processor.process(message)
      logger.trace("process: done")
    end

    private
    def ensure_database
      enforce_umask
      database_path = @options[:database]
      return if File.exist?(database_path)
      FileUtils.mkdir_p(File.dirname(database_path))
      context = Groonga::Context.new
      begin
        context.create_database(database_path) do
        end
      ensure
        context.close
      end
    end

    def enforce_umask
      File.umask(022)
    end

    def start_supervisor
      @supervisor = ServerEngine::Supervisor.new(Server, Worker) do
        force_options = {
          :worker_type   => "process",
          :workers       => @options[:n_workers],
          :log_level     => logger.level,
          :server_process_name => "Server[#{@options[:database]}] #$0",
          :worker_process_name => "Worker[#{@options[:database]}] #$0",
          :job_receive_socket_path => @job_pusher.socket_path,
          :job_pusher => @job_pusher,
        }
        @options.merge(force_options)
      end
      @supervisor_thread = Thread.new do
        @supervisor.main
      end
    end

    def shutdown_supervisor
      logger.trace("supervisor: shutdown: start")
      @supervisor.stop(true)
      logger.trace("supervisor: shutdown: stopped")
      @supervisor_thread.join
      logger.trace("supervisor: shutdown: done")
    end

    private
    def log_tag
      "slice"
    end
  end
end
