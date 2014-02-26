# Copyright (C) 2013-2014 Droonga Project
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

require "droonga/catalog/base"

module Droonga
  module Catalog
    class Version2 < Base
      def initialize(data, path)
        super
        prepare_data
      end

      def get_partitions(name)
        slices(name)
      end

      def slices(name)
        device = "."
        pattern = Regexp.new("^#{name}\.")
        results = {}
        @data["datasets"].each do |dataset_name, dataset|
          n_workers = dataset["nWorkers"]
          plugins = dataset["plugins"]
          dataset["replicas"].each do |replica|
            replica["slices"].each do |slice|
              volume_address = slice["volume"]["address"]
              if pattern =~ volume_address
                path = File.join([device, $POSTMATCH, "db"])
                path = File.expand_path(path, base_path)
                options = {
                  :dataset => dataset_name,
                  :database => path,
                  :n_workers => n_workers,
                  :plugins => plugins
                }
                results[volume_address] = options
              end
            end
          end
        end
        results
      end

      def get_routes(name, args)
        routes = []
        dataset = dataset(name)
        case args["type"]
        when "broadcast"
          replicas = select_replicas(dataset["replicas"], args["replica"])
          replicas.each do |replica|
            slices = select_slices(replica)
            slices.each do |slice|
              routes << slice["volume"]["address"]
            end
          end
        when "scatter"
          replicas = select_replicas(dataset["replicas"], args["replica"])
          replicas.each do |replica|
            slice = select_slice(replica, args["key"])
            routes << slice["volume"]["address"]
          end
        end
        routes
      end

      private
      def validate
        # TODO: Implement me.
      end

      def prepare_data
        @data["datasets"].each do |name, dataset|
          replicas = dataset["replicas"]
          replicas.each do |replica|
            total_weight = compute_total_weight(replica)
            continuum = []
            slices = replica["slices"]
            n_partitions = slices.size
            slices.each do |slice|
              weight = slice["weight"] || default_weight
              points = n_partitions * 160 * weight / total_weight
              points.times do |point|
                hash = Digest::SHA1.hexdigest("#{name}:#{point}")
                continuum << [hash[0..7].to_i(16), slice]
              end
            end
            replica["continuum"] = continuum.sort do |a, b|
              a[0] - b[0]
            end
          end
        end
      end

      def default_weight
        1
      end

      def compute_total_weight(replica)
        slices = replica["slices"]
        slices.reduce(0) do |result, slice|
          result + (slice["weight"] || default_weight)
        end
      end

      def select_replicas(replicas, how)
        case how
        when "top"
          [replicas.first]
        when "random"
          [replicas.sample]
        when "all"
          replicas
        end
      end

      def select_slices(replica, range=0..-1)
        sorted_slices = replica["slices"].sort_by do |slice|
          slice["label"]
        end
        sorted_slices[range]
      end

      def select_slice(replica, key)
        continuum = replica["continuum"]
        return replica["slices"].first unless continuum

        hash = Zlib.crc32(key)
        min = 0
        max = continuum.size - 1
        while (min < max) do
          index = (min + max) / 2
          value, key = continuum[index]
          return key if value == hash
          if value > hash
            max = index
          else
            min = index + 1
          end
        end
        continuum[max][1]
      end
    end
  end
end
