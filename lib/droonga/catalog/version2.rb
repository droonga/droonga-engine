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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "pathname"
require "fileutils"

require "droonga/address"
require "droonga/catalog/base"
require "droonga/catalog/dataset"
require "droonga/catalog/version2_validator"

module Droonga
  module Catalog
    class Version2 < Base
      def initialize(data, path)
        super
        validate
        prepare_data
      end

      def datasets
        @datasets
      end

      def slices(node)
        device = "."
        results = {}
        @datasets.each do |dataset_name, dataset|
          n_workers = dataset.n_workers
          plugins = dataset.plugins
          dataset.replicas.each do |volume|
            volume.slices.each do |slice|
              volume_address = slice.volume.address
              if volume_address.node == node
                name = volume_address.name
                path = Path.databases(base_path) +
                         device + name + "db"
                migrate_database_location(path, device, name)

                options = {
                  :dataset => dataset_name,
                  :database => path.to_s,
                  :n_workers => n_workers,
                  :plugins => plugins
                }
                results[volume_address.to_s] = options
              end
            end
          end
        end
        results
      end

      def all_nodes
        @all_nodes ||= collect_all_nodes
      end

      private
      def validate
        validator = Version2Validator.new(@data, @path)
        validator.validate
      end

      def prepare_data
        @datasets = {}
        @data["datasets"].each do |name, dataset|
          @datasets[name] = Dataset.new(name, dataset)
        end
      end

      def collect_all_nodes
        nodes = []
        @datasets.each do |name, dataset|
          nodes += dataset.all_nodes
        end
        nodes.sort.uniq
      end
    end
  end
end
