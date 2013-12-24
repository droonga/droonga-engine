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

require "droonga/event_loop"
require "droonga/handler"
require "droonga/message_receiver"

module Droonga
  module Worker
    def initialize
      @loop = EventLoop.new
      @handler = Handler.new(@loop, config.merge(:dispatcher => nil))
      receiver_socket = config[:message_receiver]
      @message_receiver = MessageReceiver.new(@loop, receiver_socket) do |message|
        process(message)
      end
    end

    def run
      $log.trace("#{log_tag}: run: start")
      @handler.start
      @message_receiver.start
      @loop.run
      @handler.shutdown
      $log.trace("#{log_tag}: run: done")
    end

    def stop
      $log.trace("#{log_tag}: stop: start")
      @message_receiver.shutdown
      @loop.stop
      $log.trace("#{log_tag}: stop: done")
    end

    private
    def process(message)
      $log.trace("#{log_tag}: process: start")
      @handler.process(message)
      $log.trace("#{log_tag}: process: done")
    end

    def log_tag
      "[#{Process.ppid}][#{Process.pid}] worker"
    end
  end
end
