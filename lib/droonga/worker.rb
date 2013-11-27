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

require "droonga/handler"

module Droonga
  module Worker
    def initialize
      @database_name = config[:database]
      @queue_name = config[:queue_name] || "DroongaQueue"
    end

    def run
      $log.trace("#{log_tag}: run: start")
      handler = Handler.new(config)
      job_queue = JobQueue.open(@database_name, @queue_name)
      @running = true
      while @running
        process(handler, job_queue)
      end
      handler.shutdown
      job_queue.close
      $log.trace("#{log_tag}: run: done")
    end

    def stop
      $log.trace("#{log_tag}: stop: start")
      @running = false
      $log.trace("#{log_tag}: stop: done")
    end

    private
    def process(handler, job_queue)
      $log.trace("#{log_tag}: process: start")
      envelope = job_queue.pull_message
      unless envelope
        $log.trace("#{log_tag}: process: abort: no message")
        return
      end
      handler.process(envelope)
      $log.trace("#{log_tag}: process: done")
    end

    def log_tag
      "[#{Process.ppid}][#{Process.pid}] worker"
    end
  end
end
