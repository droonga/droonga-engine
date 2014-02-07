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

module Droonga
  module LegacyPluggable
    class UnknownPlugin < StandardError
      attr_reader :command

      def initialize(command)
        @command = command
      end
    end

    def shutdown
      $log.trace("#{log_tag}: shutdown: plugin: start")
      @plugins.each do |plugin|
        plugin.shutdown
      end
      $log.trace("#{log_tag}: shutdown: plugin: done")
    end

    def processable?(command)
      not find_plugin(command).nil?
    end

    def process(command, *arguments)
      plugin = find_plugin(command)
      $log.trace("#{log_tag}: process: start: <#{command}>",
                 :plugin => plugin.class)
      raise UnknownPlugin.new(command) if plugin.nil?
      result = plugin.process(command, *arguments)
      $log.trace("#{log_tag}: process: done: <#{command}>",
                 :plugin => plugin.class)
      result
    end

    private
    def load_plugins(names)
      @plugins = names.collect do |name|
        plugin = instantiate_plugin(name)
        if plugin.nil?
          raise "unknown plugin: <#{name}>: TODO: improve error handling"
        end
        plugin
      end
    end

    def find_plugin(command)
      @plugins.find do |plugin|
        plugin.processable?(command)
      end
    end
  end
end
