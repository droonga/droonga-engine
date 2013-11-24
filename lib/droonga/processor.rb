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

require "droonga/job_queue"
require "droonga/handler"

module Droonga
  class Processor
    def initialize(options={})
      @options = options
      @database_name = @options[:database]
      @queue_name = @options[:options] || "DroongaQueue"
      @n_workers = @options[:n_workers] || 0
    end

    def start
      Droonga::JobQueue.ensure_schema(@database_name,
                                      @queue_name)
      @job_queue = JobQueue.open(@database_name, @queue_name)
      @handler = Handler.new(@options)
    end

    def shutdown
      $log.trace("processor: shutdown: start")
      @handler.shutdown
      @job_queue.close
      $log.trace("processor: shutdown: done")
    end

    def process(envelope, synchronous=nil)
      $log.trace("proessor: process: start")
      reply_to = envelope["replyTo"]
      command = envelope["type"]
      if @handler.processable?(command)
        $log.trace("proessor: process: handlable: #{command}")
        if synchronous.nil?
          synchronous = @handler.prefer_synchronous?(command)
        end
        message = ["", Time.now.to_f, envelope]
        if @n_workers.zero? or synchronous
          @handler.process(message)
        else
          @job_queue.push_message(message)
        end
      else
        $log.trace("proessor: process: ignore #{command}")
      end
      $log.trace("proessor: process: done")
    end
  end
end
