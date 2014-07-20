# Copyright (C) 2014 Droonga Project
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

require "droonga/plugin_registry"
require "droonga/single_step_definition"
require "droonga/adapter"
require "droonga/planner"
require "droonga/handler"
require "droonga/collector"
require "droonga/collectors"

module Droonga
  module Plugin
    class << self
      def registry
        @@registry ||= PluginRegistry.new
      end
    end

    def register(name)
      Plugin.registry.register(name, self)
    end

    def define_single_step(&block)
      single_step_definitions << SingleStepDefinition.new(self, &block)
    end

    def single_step_definitions
      @single_step_definitions ||= []
    end
  end
end
