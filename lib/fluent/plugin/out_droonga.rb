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

require "droonga/engine"

module Fluent
  class DroongaOutput < Output
    Plugin.register_output("droonga", self)

    config_param :name, :string, :default => ""
    config_param :proxy, :bool, :default => false
    config_param :n_workers, :integer, :default => 0
    config_param :database, :string, :default => ""
    config_param :queue_name, :string, :default => "DroongaQueue"
    config_param :handlers, :default => [] do |value|
      value.split(/\s*,\s*/)
    end

    def start
      super
      @engine = Droonga::Engine.new(:database => @database,
                                    :queue_name => @queue_name,
                                    :n_workers => @n_workers,
                                    :handlers => @handlers,
                                    :name => @name,
                                    :proxy => @proxy)
      @engine.start
    end

    def shutdown
      @engine.shutdown
      super
    end

    def emit(tag, es, chain)
      es.each do |time, record|
        @engine.emit(tag, time, record)
      end
      chain.next
    end
  end
end
