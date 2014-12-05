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

require "droonga/catalog/volume"

module Droonga
  module Catalog
    class Replicas
      class << self
        def create(dataset, raw_replicas)
          replicas = raw_replicas.collect do |raw_replica|
            Replica.new(dataset, raw_replica)
          end
          new(dataset, replicas)
        end
      end

      include Enumerable

      def initialize(dataset, replicas)
        @dataset = dataset
        @replicas = replicas
      end

      def each(&block)
        @replicas.each(&block)
      end

      def ==(other)
        other.is_a?(self.class) and
          to_a == other.to_a
      end

      def eql?(other)
        self == other
      end

      def hash
        to_a.hash
      end

      def select(how=nil, live_nodes=nil)
        replicas = live_replicas(live_nodes)
        case how
        when :top
          [replicas.first]
        when :random
          [replicas.sample]
        when :all
          @replicas
        else
          super
        end
      end

      def all_nodes
        @all_nodes ||= collect_all_nodes
      end

      def live_replicas(live_nodes=nil)
        return @replicas unless live_nodes

        @replicas.select do |replica|
          dead_nodes = replica.all_nodes - live_nodes
          dead_nodes.empty?
        end
      end

      private
      def collect_all_nodes
        nodes = []
        @replicas.each do |replica|
          nodes += replica.all_nodes
        end
        nodes.sort.uniq
      end
    end
  end
end
