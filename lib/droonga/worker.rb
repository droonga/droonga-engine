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
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

require "droonga/event_loop"
require "droonga/handler_runner"
require "droonga/job_receiver"

module Droonga
  module Worker
    def initialize
      @raw_loop = Coolio::Loop.new
      @loop = EventLoop.new(@raw_loop)
      @handler_runner = HandlerRunner.new(@loop,
                                          config.merge(:dispatcher => nil))
      receive_socket_path = config[:job_receive_socket_path]
      @job_receiver = JobReceiver.new(@loop, receive_socket_path) do |message|
        process(message)
      end
    end

    def run
      Droonga.logger.trace("#{log_tag}: run: start")
      @handler_runner.start
      @job_receiver.start
      @raw_loop.run
      @handler_runner.shutdown
      Droonga.logger.trace("#{log_tag}: run: done")
    end

    def stop
      Droonga.logger.trace("#{log_tag}: stop: start")
      @job_receiver.shutdown
      @raw_loop.stop
      @loop.break_current_loop
      Droonga.logger.trace("#{log_tag}: stop: done")
    end

    private
    def process(message)
      Droonga.logger.trace("#{log_tag}: process: start")
      @handler_runner.process(message)
      Droonga.logger.trace("#{log_tag}: process: done")
    end

    def log_tag
      "[#{Process.ppid}][#{Process.pid}] worker"
    end
  end
end
