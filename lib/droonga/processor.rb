# Copyright (C) 2013-2014 Droonga Project
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "droonga/loggable"
require "droonga/handler_runner"

module Droonga
  class Processor
    include Loggable

    def initialize(loop, job_pusher, options={})
      @loop = loop
      @job_pusher = job_pusher
      @options = options
      @n_workers = @options[:n_workers] || 0
    end

    def start
      @handler_runner = HandlerRunner.new(@loop, @options)
      @handler_runner.start
    end

    def shutdown
      logger.trace("shutdown: start")
      @handler_runner.shutdown
      logger.trace("shutdown: done")
    end

    def process(message)
      logger.trace("process: start")
      type = message["type"]
      if @handler_runner.processable?(type)
        logger.trace("process: handlable: #{type}")
        synchronous = @handler_runner.prefer_synchronous?(type)
        if @n_workers.zero? or synchronous
          @handler_runner.process(message)
          if synchronous
            @job_pusher.broadcast(database_reopen_message)
          end
        else
          @job_pusher.push(message)
        end
      else
        logger.trace("process: ignore #{type}")
      end
      logger.trace("process: done")
    end

    private
    def database_reopen_message
      {
        "type" => "database.reopen",
      }
    end

    def log_tag
      "processor"
    end
  end
end
