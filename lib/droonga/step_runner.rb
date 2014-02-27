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

require "droonga/plugin"
require "droonga/single_step"

module Droonga
  class StepRunner
    def initialize(plugins)
      @definitions = {}
      plugins.each do |name|
        plugin = Plugin.registry[name]
        plugin.single_step_definitions.each do |definition|
          @definitions[definition.name] = definition
        end
      end
    end

    def shutdown
    end

    def plan(message)
      type = message["type"]
      $log.trace("#{log_tag}: plan: start",
                 :dataset => message["dataset"],
                 :type => type)
      definition = find(type)
      if definition.nil?
        raise UnsupportedMessageError.new(:planner, message)
      end
      step = SingleStep.new(definition)
      plan = step.plan(message)
      $log.trace("#{log_tag}: plan: done",
                 :dataset => message["dataset"],
                 :type => type)
      plan
    end

    def find(type)
      @definitions[type]
    end

    private
    def log_tag
      "step-runner"
    end
  end
end
