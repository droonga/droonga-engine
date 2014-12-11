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

      def initialize(name, raw_dataset)
        @name = name
        @raw_dataset = raw_dataset
        @schema = nil
      end

      # provided for compatibility
      def [](key)
        @raw_dataset[key]
      end

      # provided for compatibility
      def []=(key, value)
        @raw_dataset[key] = value
      end

      def schema
        @schema ||= Schema.new(@name, @raw_dataset["schema"])
      end

      def plugins
        @raw_dataset["plugins"] || []
      end

      def fact
        @raw_dataset["fact"]
      end

      def n_workers
        @raw_dataset["nWorkers"] || 0
      end

      #XXX Currently, dataset has a property named "replicas" so
      #    can be parsed as a ReplicasVolume.
      #    We must introduce a new property "volume" to provide
      #    ReplicasVolume safely.
      def replicas
        @replicas ||= ReplicasVolume.new(self, @raw_dataset)
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
