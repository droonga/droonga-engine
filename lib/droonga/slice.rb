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

require "droonga/loggable"
require "droonga/deferrable"
require "droonga/supervisor"
require "droonga/event_loop"
require "droonga/job_pusher"
require "droonga/processor"
require "droonga/schema_applier"

module Droonga
  class Slice
    include Loggable
    include Deferrable

    def initialize(label, dataset, loop, options={})
      @label = label
      @dataset = dataset
      @loop = loop
      @options = options
      @n_workers = @options[:n_workers] || 0
      @database_path = @options[:database]
      @job_pusher = JobPusher.new(@loop, @database_path)
      @processor = Processor.new(@loop, @job_pusher, @options)
      @supervisor = nil
    end

    def start
      ensure_database
      @processor.start
      @job_pusher.start
      start_supervisor
    end

    def stop_gracefully
      logger.trace("stop_gracefully: start")
      on_stop = lambda do
        @job_pusher.shutdown
        @processor.shutdown
        yield if block_given?
        logger.trace("stop_gracefully: done")
      end
      if @supervisor
        @supervisor.stop_gracefully do
          on_stop.call
        end
      else
        on_stop.call
      end
    end

    def stop_immediately
      logger.trace("stop_immediately: start")
      @supervisor.stop_immediately if @supervisor
      @job_pusher.shutdown
      @processor.shutdown
      logger.trace("stop_immediately: done")
    end

    def refresh_node_reference
      logger.trace("refresh_node_reference: start")
      @supervisor.refresh_node_reference if @supervisor
      logger.trace("refresh_node_reference: done")
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
      config.label = @label
      config.dataset = @dataset
      config.database_path = @database_path
      config.plugins = @options[:plugins]
      config.job_pusher = @job_pusher
      config.internal_connection_lifetime = @options[:internal_connection_lifetime]
      @supervisor = Supervisor.new(@loop, @n_workers, config)
      @supervisor.on_ready = lambda do
        on_ready
      end
      @supervisor.start
    end

    def log_tag
      "slice"
    end
  end
end
