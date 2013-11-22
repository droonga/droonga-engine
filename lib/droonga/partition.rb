# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Droonga Project
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

require "droonga/server"
require "droonga/worker"
require "droonga/executor"

module Droonga
  class Partition
    DEFAULT_OPTIONS = {
      :queue_name => "DroongaQueue",
      :n_workers  => 0,
    }

    def initialize(options={})
      @options = DEFAULT_OPTIONS.merge(options)
    end

    def start
      if @options[:database] && !@options[:database].empty?
        Droonga::JobQueue.ensure_schema(@options[:database],
                                        @options[:queue_name])
      end
      start_supervisor if @options[:n_workers] > 0
      @executor = Executor.new(@options.merge(:standalone => true))
    end

    def shutdown
      $log.trace("partition: shutdown: start")
      @executor.shutdown if @executor
      shutdown_supervisor if @supervisor
      $log.trace("partition: shutdown: done")
    end

    def emit(tag, time, record, synchronous=nil)
      $log.trace("[#{Process.pid}] tag: <#{tag}> caller: <#{caller.first}>")
      @executor.dispatch(tag, time, record, synchronous)
    end

    private
    def start_supervisor
      @supervisor = ServerEngine::Supervisor.new(Server, Worker) do
        force_options = {
          :worker_type   => "process",
          :workers       => @options[:n_workers],
          :log_level     => $log.level,
          :server_process_name => "Server[#{@options[:database]}] #$0",
          :worker_process_name => "Worker[#{@options[:database]}] #$0"
        }
        @options.merge(force_options)
      end
      @supervisor_thread = Thread.new do
        @supervisor.main
      end
    end

    def shutdown_supervisor
      $log.trace("supervisor: shutdown: start")
      @supervisor.stop(true)
      $log.trace("supervisor: shutdown: stopped")
      @supervisor_thread.join
      $log.trace("supervisor: shutdown: done")
    end
  end
end
