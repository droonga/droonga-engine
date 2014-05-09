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

      def slices(name)
        device = "."
        pattern = Regexp.new("^#{name}\.")
        results = {}
        @datasets.each do |dataset_name, dataset|
          n_workers = dataset.n_workers
          plugins = dataset.plugins
          dataset.replicas.each do |volume|
            volume.slices.each do |slice|
              volume_address = slice.volume.address
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

      def all_nodes
        nodes = []
        @datasets.each do |name, dataset|
          nodes += dataset.all_nodes
        end
        nodes.sort.uniq
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
    end
  end
end
