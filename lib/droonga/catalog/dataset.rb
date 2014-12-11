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

      def initialize(name, raw)
        @name = name
        @raw = raw
        @schema = nil
      end

      # provided for compatibility
      def [](key)
        @raw[key]
      end

      # provided for compatibility
      def []=(key, value)
        @raw[key] = value
      end

      def schema
        @schema ||= Schema.new(@name, @raw["schema"])
      end

      def plugins
        @raw["plugins"] || []
      end

      def fact
        @raw["fact"]
      end

      def n_workers
        @raw["nWorkers"] || 0
      end

      #XXX Currently, dataset has a property named "replicas" so
      #    can be parsed as a ReplicasVolume.
      #    We must introduce a new property "volume" to provide
      #    ReplicasVolume safely.
      def replicas
        @replicas ||= ReplicasVolume.new(self, @raw)
      end

      def all_nodes
        @all_nodes ||= replicas.all_nodes
      end

      def compute_routes(message, live_nodes)
        @replicas.compute_routes(message, live_nodes)
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
