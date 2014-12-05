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
        collect_routes_from_replicas(replicas,
                                     :message    => message,
                                     :live_nodes => live_nodes)
      end

      def single_slice?
        # TODO: Support slice key
        replicas.all? do |replica|
          replica.is_a?(SingleVolume) or
            replica.slices.nil? or
            replica.slices.size == 1
        end
      end

      private
      def collect_routes_from_replicas(replicas, params)
        message = params[:message]
        routes = params[:routes] ||= []
        case message["type"]
        when "broadcast"
          replicas = replicas.select(message["replica"].to_sym, params[:live_nodes])
          replicas.each do |replica|
            slices = replica.select_slices
            collect_routes_from_slices(slices, params)
          end
        when "scatter"
          replicas = replicas.select(message["replica"].to_sym, params[:live_nodes])
          replicas.each do |replica|
            slice = replica.choose_slice(message["record"])
            collect_routes_from_slice(slice, params)
          end
        end
        routes
      end

      def collect_routes_from_slices(slices, params)
        slices.each do |slice|
          collect_routes_from_slice(slice, params)
        end
      end

      def collect_routes_from_slice(slice, params)
        if slice.replicas
          collect_routes_from_replicas(slice.replicas, params)
        else
          routes = params[:routes] ||= []
          routes << slice.volume.address.to_s
          routes
        end
      end
    end
  end
end
