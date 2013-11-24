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
require "droonga/processor"

module Droonga
  class Partition
    def initialize(options={})
      @options = options
      @n_workers = @options[:n_workers] || 0
      @processor = Processor.new(@options)
      @supervisor = nil
    end

    def start
      start_supervisor if @n_workers > 0
      @processor.start
    end

    def shutdown
      $log.trace("partition: shutdown: start")
      @processor.shutdown
      shutdown_supervisor if @supervisor
      $log.trace("partition: shutdown: done")
    end

    def process(envelope, synchronous=nil)
      $log.trace("partition: process: start")
      @processor.process(envelope, synchronous)
      $log.trace("partition: process: done")
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
