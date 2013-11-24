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

require "droonga/adapter_plugin"

module Droonga
  class Adapter
    def initialize(executor, options={})
      @executor = executor
      load_plugins(options[:adapters] || [])
    end

    def shutdown
      $log.trace("#{log_tag}: shutdown: start")
      @plugins.each do |plugin|
        plugins.shutdown
      end
      $log.trace("#{log_tag}: shutdown: done")
    end

    def processable?(command)
      not find_plugin(command).nil?
    end

    def process(command, body)
      plugin = find_plugin(command)
      $log.trace("#{log_tag}: process: start: <#{command}>",
                 :plugin => plugin.class)
      plugin.process(command, body)
      $log.trace("#{log_tag}: process: done: <#{command}>",
                 :plugin => plugin.class)
    end

    private
    def load_plugins(names)
      @plugins = names.collect do |name|
        AdapterPlugin.repository.instantiate(name, @executor)
      end
    end

    def find_plugin(command)
      @plugins.find do |plugin|
        plugin.processable?(command)
      end
    end

    def log_tag
      "adapter"
    end
  end
end
