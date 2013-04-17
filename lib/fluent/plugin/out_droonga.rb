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

module Fluent
  class DroongaOutput < Output
    Plugin.register_output("droonga", self)

    config_param :n_workers, :integer, :default => 1
    config_param :database, :string, :default => "droonga/db"
    config_param :queue_name, :string, :default => "DroongaQueue"
    config_param :handlers, :default => [] do |value|
      value.split(/\s*,\s*/)
    end

    def start
      super
      @worker = Droonga::Worker.new(:database => @database,
                                    :queue_name => @queue_name,
                                    :pool_size => @n_workers,
                                    :handlers => @handlers)
    end

    def shutdown
      super
      @worker.shutdown
    end

    def emit(tag, es, chain)
      es.each do |time, record|
        @worker.dispatch(tag, time, record)
      end
      chain.next
    end
  end
end
