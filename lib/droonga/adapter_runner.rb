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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "droonga/loggable"
require "droonga/message_matcher"
require "droonga/input_message"
require "droonga/output_message"
require "droonga/adapter"

module Droonga
  class AdapterRunner
    include Loggable

    def initialize(dispatcher, plugins)
      @dispatcher = dispatcher
      default_plugins = ["error"]
      plugins += (default_plugins - plugins)
      logger.debug("activating plugins: #{plugins.join(", ")}")
      @adapter_classes = Adapter.find_sub_classes(plugins)
      logger.debug("activated:\n#{@adapter_classes.join("\n")}")
    end

    def shutdown
    end

    def adapt_input(message)
      logger.trace("adapt_input: start",
                   :dataset => message["dataset"],
                   :type => message["type"])
      adapted_message = message
      adapted_message["appliedAdapters"] = []
      @adapter_classes.each do |adapter_class|
        adapter_class_id = adapter_class.id
        pattern = adapter_class.input_message.pattern
        if pattern
          matcher = MessageMatcher.new(pattern)
          logger.trace("adapt_input: skip: #{adapter_class_id}",
                       :pattern => pattern)
          next unless matcher.match?(adapted_message)
        end
        logger.trace("adapt_input: use: #{adapter_class_id}")
        input_message = InputMessage.new(adapted_message)
        adapter = adapter_class.new
        adapter.adapt_input(input_message)
        adapted_message = input_message.adapted_message
        adapted_message["appliedAdapters"] << adapter_class_id
      end
      logger.trace("adapt_input: done",
                   :dataset => adapted_message["dataset"],
                   :type => adapted_message["type"])
      adapted_message
    end

    def adapt_output(message)
      logger.trace("adapt_output: start",
                   :dataset => message["dataset"],
                   :type => message["type"])
      adapted_message = message
      applied_adapters = adapted_message["appliedAdapters"]
      @adapter_classes.reverse_each do |adapter_class|
        adapter_class_id = adapter_class.id
        if applied_adapters
          logger.trace("adapt_output: skip: #{adapter_class_id}: " +
                         "input adapter wasn't applied",
                       :applied_adapters => applied_adapters)
          next unless applied_adapters.include?(adapter_class.id)
        end
        pattern = adapter_class.output_message.pattern
        if pattern
          matcher = MessageMatcher.new(pattern)
          logger.trace("adapt_output: skip: #{adapter_class_id}",
                       :pattern => pattern)
          next unless matcher.match?(adapted_message)
        end
        logger.trace("adapt_output: use: #{adapter_class_id}")
        output_message = OutputMessage.new(adapted_message)
        adapter = adapter_class.new
        adapter.adapt_output(output_message)
        adapted_message = output_message.adapted_message
      end
      logger.trace("adapt_output: done",
                   :dataset => adapted_message["dataset"],
                   :type => adapted_message["type"])
      adapted_message
    end

    private
    def log_tag
      "adapter-runner"
    end
  end
end
