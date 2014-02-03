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

require "droonga/command"
require "droonga/command_repository"
require "droonga/plugin_repository"

module Droonga
  module PluginRegisterable
    class << self
      def extended(plugin_class)
        super
        plugin_class.class_variable_set(:@@repository, PluginRepository.new)
      end
    end

    def repository
      class_variable_get(:@@repository)
    end

    def inherited(sub_class)
      super
      sub_class.instance_variable_set(:@command_repository,
                                      CommandRepository.new)
    end

    def command(method_name_or_map, options={})
      if method_name_or_map.is_a?(Hash)
        type, method_name = method_name_or_map.to_a.first
        options[:pattern] ||= ["type", :equal, type.to_s]
      else
        method_name = method_name_or_map
        options[:pattern] ||= ["type", :equal, method_name.to_s]
      end
      command = Command.new(method_name, options)
      @command_repository.register(command)
    end

    def commands
      @command_repository.commands
    end

    def find_command(message)
      @command_repository.find(message)
    end

    def method_name(message)
      message = {"type" => message.to_s} unless message.is_a?(Hash)
      command = find_command(message)
      return nil if command.nil?
      command.method_name
    end

    def processable?(message)
      message = {"type" => message.to_s} unless message.is_a?(Hash)
      command = find_command(message)
      not command.nil?
    end
  end
end
