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

require "tsort"

module Droonga
  class Distributor
    class UndefinedInputError < StandardError
      attr_reader :input
      def initialize(input)
        @input = input
        super("undefined input assigned: <#{input}>")
      end
    end

    class CyclicStepsError < StandardError
      attr_reader :steps
      def initialize(steps)
        @steps = steps
        super("cyclic steps found: <#{steps}>")
      end
    end

    include TSort

    def initialize(dispatcher, plan)
      @dispatcher = dispatcher
      @plan = plan
      build_dependencies
    end

    def distribute
      steps = []
      each_strongly_connected_component do |nodes|
        raise CyclicStepsError.new(nodes) if nodes.size > 1
        nodes.each do |node|
          steps << @step_maps[node] if node.is_a?(Integer)
        end
      end
      @dispatcher.dispatch_steps(steps)
    end

    private
    def build_dependencies
      @dependencies = {}
      @step_maps = {}
      step_id = 0
      @plan.each do |step|
        step_id += 1
        # Integer#hash (step_id.hash) is very faster than Hash#hash (step.hash).
        @step_maps[step_id] = step
        @dependencies[step_id] = step["inputs"]
        next unless step["outputs"]
        step["outputs"].each do |output|
          @dependencies[output] = [step_id]
        end
      end
    end

    def tsort_each_node(&block)
      @dependencies.each_key(&block)
    end

    def tsort_each_child(node, &block)
      if node.is_a? String and @dependencies[node].nil?
        raise UndefinedInputError.new(node)
      end
      if @dependencies[node]
        @dependencies[node].each(&block)
      end
    end
  end
end
