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

module Droonga
  class SingleStepDefinition
    attr_accessor :name
    attr_accessor :handler
    attr_accessor :collector
    attr_writer :write, :single_operation
    attr_writer :timeout_seconds_calculator
    attr_accessor :inputs
    attr_accessor :output
    def initialize(plugin_module)
      @plugin_module = plugin_module
      @name = nil
      @handler = nil
      @collector = nil
      @write = false
      @single_operation = false
      @timeout_seconds_calculator = lambda do |step|
        if step["timeout"]
          return step["timeout"]
        elsif step["body"]
          return step["body"]["timeout"] if step["body"]["timeout"]
        end
        nil
      end

      @inputs = []
      @output = {}
      yield(self)
    end

    def write?
      @write
    end

    def single_operation?
      return false if @write
      @single_operation
    end

    def timeout_seconds_for_step(step)
      if @timeout_seconds_calculator
        @timeout_seconds_calculator.call(step)
      else
        nil
      end
    end

    def handler_class
      resolve_class(@handler)
    end

    def collector_class
      resolve_class(@collector)
    end

    private
    def resolve_class(target)
      return nil if target.nil?
      return target if target.is_a?(Class)
      @plugin_module.const_get(target)
    end
  end
end
