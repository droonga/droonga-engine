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

require "droonga/catalog/schema"
require "droonga/catalog/volume"

module Droonga
  module Catalog
    class Dataset
      DEFAULT_NAME = "Default"

      attr_reader :name

      def initialize(name, data)
        @name = name
        @data = data
        @schema = nil
      end

      # provided for compatibility
      def [](key)
        @data[key]
      end

      # provided for compatibility
      def []=(key, value)
        @data[key] = value
      end

      def schema
        @schema ||= Schema.new(@name, @data["schema"])
      end

      def plugins
        @data["plugins"] || []
      end

      def fact
        @data["fact"]
      end

      def n_workers
        @data["nWorkers"] || 0
      end

      #XXX Currently, dataset has a property named "replicas" so
      #    can be parsed as a ReplicasVolume.
      #    We must introduce a new property "volume" to provide
      #    ReplicasVolume safely.
      def replicas
        @replicas ||= ReplicasVolume.new(self, @data)
      end

      def all_nodes
        @all_nodes ||= replicas.all_nodes
      end

      def compute_routes(message, live_nodes)
        @replicas.collect_routes_for(message,
                                     :live_nodes => live_nodes)
      end

      def sliced?
        # TODO: Support slice key
        replicas.any? do |volume|
          volume.sliced?
        end
      end
    end
  end
end
