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

require "droonga/loggable"
require "droonga/supervisor"
require "droonga/event_loop"
require "droonga/job_pusher"
require "droonga/processor"
require "droonga/schema_applier"

module Droonga
  class Slice
    include Loggable

    attr_accessor :on_ready
    def initialize(dataset, loop, options={})
      @dataset = dataset
      @loop = loop
      @options = options
      @n_workers = @options[:n_workers] || 0
      @database_path = @options[:database]
      @job_pusher = JobPusher.new(@loop, @database_path)
      @processor = Processor.new(@loop, @job_pusher, @options)
      @supervisor = nil
      @on_ready = nil
    end

    def start
      ensure_database
      @processor.start
      @job_pusher.start
      start_supervisor
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
      context = Groonga::Context.new
      begin
        if File.exist?(@database_path)
          context.open_database(@database_path) do
            apply_schema(context)
          end
        else
          FileUtils.mkdir_p(File.dirname(@database_path))
          context.create_database(@database_path) do
            apply_schema(context)
          end
        end
      ensure
        context.close
      end
    end

    def enforce_umask
      File.umask(022)
    end

    def apply_schema(context)
      applier = SchemaApplier.new(context, @dataset.schema)
      applier.apply
    end

    def start_supervisor
      if @n_workers.zero?
        on_ready
        return
      end

      config = Supervisor::WorkerConfiguration.new
      config.name = @options[:name]
      config.dataset = @dataset
      config.database_path = @database_path
      config.plugins = @options[:plugins]
      config.job_pusher = @job_pusher
      @supervisor = Supervisor.new(@loop, @n_workers, config)
      @supervisor.on_ready = lambda do
        on_ready
      end
      @supervisor.start
    end

    def shutdown_supervisor
      logger.trace("supervisor: shutdown: start")
      @supervisor.stop_gracefully
      logger.trace("supervisor: shutdown: done")
    end

    private
    def on_ready
      @on_ready.call if @on_ready
    end

    def log_tag
      "slice"
    end
  end
end
