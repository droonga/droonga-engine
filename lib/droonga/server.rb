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

require "groonga"

module Droonga
  module Server
    def before_run
      $log.trace("#{log_tag}: before_run: start")
      # TODO: Use JobQueue object
      @context = Groonga::Context.new
      @database = @context.open_database(config[:database])
      @queue = @context[config[:queue_name]]
      $log.trace("#{log_tag}: before_run: done")
    end

    def after_run
      $log.trace("#{log_tag}: after_run: start")
      @queue.close
      @database.close
      @context.close
      $log.trace("#{log_tag}: after_run: done")
    end

    def stop(stop_graceful)
      $log.trace("#{log_tag}: stop: start")

      $log.trace("#{log_tag}: stop: queue: unblock: start")
      3.times do |i|
        $log.trace("#{log_tag}: stop: queue: unblock: #{i}: start")
        super(stop_graceful)
        @queue.unblock
        sleep(i ** 2 * 0.1)
        $log.trace("#{log_tag}: stop: queue: unblock: #{i}: done")
      end
      $log.trace("#{log_tag}: stop: queue: unblock: done")

      $log.trace("#{log_tag}: stop: done")
    end

    private
    def log_tag
      "[#{Process.ppid}][#{Process.pid}] server"
    end
  end
end
