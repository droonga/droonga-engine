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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "droonga/loggable"
require "droonga/message_matcher"
require "droonga/collector"
require "droonga/collector_message"

module Droonga
  class CollectorRunner
    include Loggable

    def initialize(plugins)
      default_plugins = ["basic"]
      plugins += (default_plugins - plugins)
      @collector_classes = Collector.find_sub_classes(plugins)
    end

    def shutdown
    end

    def collect(message)
      collector_message = CollectorMessage.new(message)
      logger.trace("collect: start",
                   :type => collector_message.type)
      collector_class = find_collector_class(message)
      if collector_class.nil?
        raise UnsupportedMessageError.new(:collector, message)
      end
      collector = collector_class.new
      collector.collect(collector_message)
      logger.trace("collector: done")
    end

    private
    def find_collector_class(message)
      @collector_classes.find do |collector_class|
        pattern = collector_class.message.pattern
        if pattern
          matcher = MessageMatcher.new(pattern)
          matcher.match?(message)
        else
          false
        end
      end
    end

    def log_tag
      "collector-runner"
    end
  end
end
