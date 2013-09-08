# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 droonga project
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
require "msgpack"
require "cool.io"

require "droonga/server"
require "droonga/worker"
require "droonga/executor"

module Droonga
  class Engine
    DEFAULT_OPTIONS = {
      :queue_name => "DroongaQueue",
      :n_workers  => 1,
      :with_server  => false
    }

    def initialize(options={})
      @options = DEFAULT_OPTIONS.merge(options)
    end

    def start
      if !@options[:database] || @options[:database].empty?
        name = @options[:name]
        database = File.join([File.basename(name), 'db'])
        @options[:database] = database
      end
      if @options[:n_workers] > 0 || @options[:with_server]
        @message_input, @message_output = IO.pipe
        @message_input.sync = true
        @message_output.sync = true
        start_supervisor
      end
      if @options[:with_server]
        start_emitter
      else
        @executor = Executor.new(@options)
      end
    end

    def shutdown
      $log.trace("engine: shutdown: start")
      shutdown_emitter if @emitter
      @executor.shutdown if @executor
      if @supervisor
        shutdown_supervisor
        @message_input.close unless @message_input.closed?
        @message_output.close unless @message_output.closed?
      end
      $log.trace("engine: shutdown: done")
    end

    def emit(tag, time, record, synchronous=nil)
      $log.trace("tag: <#{tag}>")
      if @executor
        @executor.dispatch(tag, time, record, synchronous)
      else
        @emitter.write(MessagePack.pack([tag, time, record, synchronous]))
        @loop_breaker.signal
      end
    end

    private
    def start_emitter
      @loop = Coolio::Loop.new
      @emitter = Coolio::IO.new(@message_output)
      @emitter.on_write_complete do
        $log.trace("emitter: written")
      end
      @emitter.attach(@loop)
      @loop_breaker = Coolio::AsyncWatcher.new
      @loop_breaker.attach(@loop)
      @emitter_thread = Thread.new do
        @loop.run
      end
    end

    def shutdown_emitter
      $log.trace("emitter: shutdown: start")
      @emitter.close
      $log.trace("emitter: shutdown: emitter: closed")
      @loop.stop
      @loop_breaker.signal
      $log.trace("emitter: shutdown: loop: stopped")
      @emitter_thread.join
      $log.trace("emitter: shutdown: done")
    end

    def start_supervisor
      server = @options[:with_server] ? Server : nil
      @supervisor = ServerEngine::Supervisor.new(server, Worker) do
        force_options = {
          :worker_type   => "process",
          :workers       => @options[:n_workers],
          :message_input => @message_input,
          :log_level     => $log.level,
        }
        @options.merge(force_options)
      end
      @supervisor.logger = $log
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
