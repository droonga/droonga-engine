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

require "droonga/executor"

module Droonga
  module Worker
    attr_reader :context, :envelope, :name

    def initialize
      @executor = Executor.new(config.merge(:proxy => false))
    end

    def run
      $log.trace("worker: run: start")
      @running = true
      while @running
        $log.trace("worker: run: pull_message: start")
        @executor.execute_one
        $log.trace("worker: run: pull_message: done")
      end
      @executor.shutdown
      $log.trace("worker: run: done")
    end

    def stop
      $log.trace("worker: stop: start")
      @running = false
      $log.trace("worker: stop: done")
    end

    private
    def shutdown_workers
      @pool.each do |pid|
        Process.kill(:TERM, pid)
      end
      queue = @context[@queue_name]
      3.times do |i|
        break if @pool.empty?
        queue.unblock
        @pool.reject! do |pid|
          not Process.waitpid(pid, Process::WNOHANG).nil?
        end
        sleep(i ** 2 * 0.1)
      end
      @pool.each do |pid|
        Process.kill(:KILL, pid)
      end
    end
  end
end
