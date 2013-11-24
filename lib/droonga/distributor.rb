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

require "droonga/distributor_plugin"

module Droonga
  class Distributor
    def initialize(executor, options={})
      @executor = executor
      @plugins = []
      @options = options
      # TODO: don't put the default distributions
      load_plugins(options[:distributors] || ["search", "crud", "groonga", "watch"])
    end

    def shutdown
      $log.trace("#{log_tag}: shutdown: start")
      @plugins.each do |plugin|
        plugins.shutdown
      end
      $log.trace("#{log_tag}: shutdown: done")
    end

    def distribute(envelope)
      command = envelope["type"]
      plugin = find_plugin(command)
      if plugin.nil?
        raise "unknown distributor plugin: <#{command}>: " +
                "TODO: improve error hndling"
      end
      plugin.process(envelope)
    end

    def post(message)
      @executor.post(message, "dispatcher")
    end

    private
    def load_plugins(plugin_names)
      plugin_names.each do |plugin_name|
        add_plugin(plugin_name)
      end
    end

    def add_plugin(name)
      plugin = DistributorPlugin.repository.instantiate(name, self)
      @plugins << plugin
    end

    def find_plugin(command)
      @plugins.find do |plugin|
        plugin.processable?(command)
      end
    end

    def log_tag
      "[#{Process.pid}] distributor"
    end
  end
end
