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

require "droonga/message_matcher"
require "droonga/planner"

module Droonga
  class PlannerRunner
    def initialize(dispatcher, plugins)
      @dispatcher = dispatcher
      @planner_classes = Planner.find_sub_classes(plugins)
    end

    def shutdown
    end

    def plan(message)
      $log.trace("#{log_tag}: plan: start",
                 :dataset => message["dataset"],
                 :type => message["type"])
      planner_class = find_planner_class(message)
      if planner_class.nil?
        raise UnsupportedMessageError.new(:planner, message)
      end
      planner = planner_class.new(@dispatcher)
      plan = planner.plan(message)
      $log.trace("#{log_tag}: plan: done",
                 :steps => plan.collect {|step| step["type"]})
      plan
    end

    private
    def find_planner_class(message)
      @planner_classes.find do |planner_class|
        pattern = planner_class.message.pattern
        if pattern
          matcher = MessageMatcher.new(pattern)
          matcher.match?(message)
        else
          false
        end
      end
    end

    def log_tag
      "planner-runner"
    end
  end
end
