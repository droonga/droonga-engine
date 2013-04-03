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

require "droonga/worker"
require "droonga/plugin"

module Fluent
  class DroongaOutput < Output
    Plugin.register_output("droonga", self)

    config_param :n_workers, :integer, :default => 1
    config_param :database, :string, :default => "droonga.db"
    config_param :queue_name, :string, :default => "DroongaQueue"
    config_param :handlers, :default => [] do |value|
      value.split(/\s*,\s*/)
    end

    def configure(conf)
      super
      load_handlers
    end

    def start
      super
      @workers = []
      @n_workers.times do
        pid = Process.fork
        if pid
          @workers << pid
          next
        end
        # child process
        begin
          create_worker.start
          exit! 0
        end
      end
      @worker = create_worker
    end

    def shutdown
      super
      @worker.shutdown
      @workers.each do |pid|
        Process.kill(:TERM, pid)
      end
    end

    def emit(tag, es, chain)
      es.each do |time, record|
        # Merge it if needed
        dispatch(tag, time, record)
      end
      chain.next
    end

    def dispatch(tag, time, record)
      if @workers.empty?
        @worker.process_message(record)
      else
        @worker.post_message(record)
      end
    end

    private
    def load_handlers
      @handlers.each do |handler_name|
        plugin = Droonga::Plugin.new("handler", handler_name)
        plugin.load
      end
    end

    def create_worker
      worker = Droonga::Worker.new(@database, @queue_name)
      @handlers.each do |handler_name|
        worker.add_handler(handler_name)
      end
      worker
    end
  end
end
