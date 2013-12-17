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
  class DistributionPlanner
    class UndefinedInputError < StandardError
      attr_reader :input
      def initialize(input)
        @input = input
        super("undefined input assigned: <#{input}>")
      end
    end

    class CyclicComponentsError < StandardError
      attr_reader :components
      def initialize(components)
        @components = components
        super("cyclic components found: <#{components}>")
      end
    end

    include TSort

    attr_reader :components
    def initialize(dispatcher, components)
      @dispatcher = dispatcher
      @components = components
    end

    def resolve(id)
      @dependency = {}
      @components.each do |component|
        @dependency[component] = component["inputs"]
        next unless component["outputs"]
        component["outputs"].each do |output|
          @dependency[output] = [component]
        end
      end
      @components = []
      each_strongly_connected_component do |cs|
        raise CyclicComponentsError.new(cs) if cs.size > 1
        @components.concat(cs) unless cs.first.is_a? String
      end
      resolve_routes(id)
    end

    private
    def resolve_routes(id)
      local = [id]
      destinations = Hash.new(0)
      @components.each do |component|
        dataset = component["dataset"]
        routes =
          if dataset
            Droonga.catalog.get_routes(dataset, component)
          else
            local
          end
        routes.each do |route|
          destinations[@dispatcher.farm_path(route)] += 1
        end
        component["routes"] = routes
      end
      return destinations
    end

    def tsort_each_node(&block)
      @dependency.each_key(&block)
    end

    def tsort_each_child(node, &block)
      if node.is_a? String and @dependency[node].nil?
        raise UndefinedInputError.new(node)
      end
      if @dependency[node]
        @dependency[node].each(&block)
      end
    end
  end
end
