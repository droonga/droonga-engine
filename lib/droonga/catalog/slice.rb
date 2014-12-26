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
    class Slice
      def initialize(dataset, raw)
        @dataset = dataset
        @raw = raw
      end

      def weight
        @raw["weight"] || 1
      end

      def label
        @raw["label"]
      end

      def boundary
        @raw["boundary"]
      end

      def volume
        @volume ||= Catalog::Volume.create(@dataset, @raw["volume"])
      end

      def all_nodes
        @all_nodes ||= volume.all_nodes
      end

      def compute_routes(message, active_nodes)
        volume.compute_routes(message, active_nodes)
      end
    end
  end
end
