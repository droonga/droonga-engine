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
      def initialize(dataset, data)
        @dataset = dataset
        @data = data
      end

      def weight
        @data["weight"] || 1
      end

      def label
        @data["label"]
      end

      def boundary
        @data["boundary"]
      end

      def volume
        @volume ||= Catalog::Volume.create(@dataset, @data["volume"])
      end

      def replicas
        if volume.is_a?(ReplicasVolume)
          volume.replicas
        else
          nil
        end
      end

      def slices
        if volume.is_a?(SlicesVolume)
          volume.slices
        else
          nil
        end
      end

      def all_nodes
        @all_nodes ||= volume.all_nodes
      end

      def collect_routes_for(message, params)
        if replicas
          replicas.collect_routes_for(message, params)
        elsif slices
          slices.collect_routes_for(message, params)
        else
          routes = params[:routes] ||= []
          routes << volume.address.to_s
          routes
        end
      end
    end
  end
end
