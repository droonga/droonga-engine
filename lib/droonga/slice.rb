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
require "droonga/event_loop"
require "droonga/message_pusher"
require "droonga/processor"

module Droonga
  class Slice
    def initialize(loop, options={})
      @options = options
      @n_workers = @options[:n_workers] || 0
      @loop = loop
      @message_pusher = MessagePusher.new(@loop)
      @processor = Processor.new(@loop, @message_pusher, @options)
      @supervisor = nil
    end

    def start
      ensure_database
      @processor.start
      base_path = @options[:database]
      @message_pusher.start(base_path)
      start_supervisor if @n_workers > 0
    end

    def shutdown
      $log.trace("slice: shutdown: start")
      shutdown_supervisor if @supervisor
      @message_pusher.shutdown
      @processor.shutdown
      $log.trace("slice: shutdown: done")
    end

    def process(message)
      $log.trace("slice: process: start")
      @processor.process(message)
      $log.trace("slice: process: done")
    end

    private
    def ensure_database
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

    def start_supervisor
      @supervisor = ServerEngine::Supervisor.new(Server, Worker) do
        force_options = {
          :worker_type   => "process",
          :workers       => @options[:n_workers],
          :log_level     => $log.level,
          :server_process_name => "Server[#{@options[:database]}] #$0",
          :worker_process_name => "Worker[#{@options[:database]}] #$0",
          :message_receiver => @message_pusher.raw_receiver,
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
