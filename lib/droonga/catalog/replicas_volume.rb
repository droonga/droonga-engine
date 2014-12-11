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
  module Catalog
    class ReplicasVolume
      include Enumerable

      def initialize(dataset, raw_volume)
        @dataset = dataset
        @raw_volume = raw_volume
      end

      def each(&block)
        replicas.each(&block)
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
        case how
        when :top
          replicas = live_replicas(live_nodes)
          [replicas.first]
        when :random
          replicas = live_replicas(live_nodes)
          [replicas.sample]
        when :all
          replicas
        else
          super
        end
      end

      def replicas
        @replicas ||= create_replicas
      end

      def all_nodes
        @all_nodes ||= collect_all_nodes
      end

      def live_replicas(live_nodes=nil)
        return replicas unless live_nodes

        replicas.select do |volume|
          dead_nodes = volume.all_nodes - live_nodes
          dead_nodes.empty?
        end
      end

      def compute_routes(message, live_nodes)
        routes = []
        case message["type"]
        when "broadcast"
          replicas = select(message["replica"].to_sym, live_nodes)
          replicas.each do |volume|
            routes.concat(volume.compute_routes(message, live_nodes))
          end
        when "scatter"
          replicas = select(message["replica"].to_sym, live_nodes)
          replicas.each do |volume|
            routes.concat(volume.compute_routes(message, live_nodes))
          end
        end
        routes.sort.uniq
      end

      def sliced?
        replicas.any? do |volume|
          volume.sliced?
        end
      end

      private
      def create_replicas
        @raw_volume["replicas"].collect do |raw_replica|
          Catalog::Volume.create(@dataset, raw_replica)
        end
      end

      def collect_all_nodes
        nodes = []
        replicas.each do |volume|
          nodes += volume.all_nodes
        end
        nodes.sort.uniq
      end
    end
  end
end
