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

require "msgpack"
require "cool.io"
require "groonga"

require "droonga/executor"

module Droonga
  module Server
    class Receiver
      def initialize(input)
        @input = input
      end

      def run
        $log.trace
        @io = Coolio::IO.new(@input)
        unpacker = MessagePack::Unpacker.new
        @io.on_read do |data|
          $log.trace("receiver: received: <#{data.bytesize}>")
          unpacker.feed_each(data) do |message|
            yield message
          end
        end
        @loop = Coolio::Loop.new
        @loop.attach(@io)
        @loop_breaker = Coolio::AsyncWatcher.new
        @loop.attach(@loop_breaker)
        @running = true
        @loop.run
      end

      def stop
        unless @running
          $log.trace("receiver: stop: not needed")
          return
        end

        $log.trace("receiver: stop: start")
        @io.close
        $log.trace("receiver: stop: closed")
        @loop.stop
        @running = false
        @loop_breaker.signal
        $log.trace("receiver: stop: done")
      end
    end

    def initialize
      super
      @message_input = config[:message_input]
      @executor = Executor.new(config)
      if config[:proxy]
        @executor.add_handler("proxy_message")
        @executor.add_handler("adapter")
      end
    end

    def before_run
      @receiver = Receiver.new(@message_input)
      @receiver_thread = Thread.new do
        @receiver.run do |message|
          $log.trace("received: start")
          @executor.dispatch(*message)
          $log.trace("received: done")
        end
      end
    end

    def after_run
      $log.trace("server: after_run: start")

      $log.trace("server: after_run: receiver: start")
      @receiver_thread.join
      $log.trace("server: after_run: receiver: done")

      $log.trace("server: after_run: groonga: start")
      @executor.shutdown
      $log.trace("server: after_run: groonga: done")

      $log.trace("server: after_run: done")
    end

    def stop(stop_graceful)
      $log.trace("server: stop: start")

      $log.trace("server: stop: receiver: stop: start")
      @receiver.stop
      $log.trace("server: stop: receiver: stop: done")

      $log.trace("server: stop: queue: unblock: start")
      @executor.unblock_queue
      $log.trace("server: stop: queue: unblock: done")

      $log.trace("server: stop: done")
    end
  end
end
