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
  class PluginRegistry
    include Enumerable

    def initialize
      @plugins = {}
    end

    def each(&block)
      @plugins.each(&block)
    end

    def register(name, plugin_module)
      @plugins[name] = plugin_module
    end

    def [](name)
      @plugins[name]
    end

    def clear
      @plugins.clear
    end

    def find_sub_classes(name, klass)
      plugin_module = self[name]
      return [] if plugin_module.nil?
      sub_classes = []
      collect_sub_classes_recursive(plugin_module, klass, sub_classes)
      sub_classes
    end

    private
    def collect_sub_classes_recursive(base, klass, sub_classes)
      base.constants.each do |constant_name|
        constant = base.const_get(constant_name)
        next unless constant.is_a?(Module)
        sub_classes << constant if constant < klass
        collect_sub_classes_recursive(constant, klass, sub_classes)
      end
    end
  end
end
