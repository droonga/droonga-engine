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
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

require "time"

module Droonga
  class CatalogGenerator
    DEFAULT_DATASET = "Default"
    DEFAULT_HOSTS = ["127.0.0.1"]
    DEFAULT_N_WORKERS = 4
    DEFAULT_PLUGINS = ["groonga", "search", "crud", "dump", "system"]
    DEFAULT_PORT = 10031
    DEFAULT_TAG = "droonga"

    def initialize
      @version = 2
      @effective_date = Time.now
      @datasets = {}
    end

    def add_dataset(name, options)
      @datasets[name] = Dataset.new(name, options)
    end

    def generate
      {
        "version"       => @version,
        "effectiveDate" => @effective_date.iso8601,
        "datasets"      => catalog_datasets,
      }
    end

    private
    def catalog_datasets
      catalog_datasets = {}
      @datasets.each do |name, dataset|
        catalog_datasets[name] = dataset.to_catalog
      end
      catalog_datasets
    end

    class Dataset
      def initialize(name, options)
        @name = name
        @options = options
      end

      def n_workers
        @options[:n_workers] || DEFAULT_N_WORKERS
      end

      def plugins
        @options[:plugins] || DEFAULT_PLUGINS
      end

      def schema
        @options[:schema] || {}
      end

      def fact
        @options[:fact]
      end

      def replicas
        return @options[:replicas] if @options[:replicas]
        @generated_replicas ||= Replicas.new(@options).to_json
      end

      def to_catalog
        catalog = {
          "nWorkers" => n_workers,
          "plugins"  => plugins,
          "schema"   => schema,
          "replicas" => replicas,
        }
        catalog["fact"] = fact if fact
        catalog
      end

      private
    end

    class Replicas
      def initialize(options={})
        @hosts      = options[:hosts] || DEFAULT_HOSTS
        @port       = options[:port]
        @tag        = options[:tag]
        @n_slices   = options[:n_slices]
      end

      def to_json
        @json ||= generate_json
      end

      private
      def generate_json
        replicas = []
        @hosts.each do |host|
          replica = Replica.new(host, :port => @port,
                                      :tag => @tag,
                                      :n_slices => @n_slices)
          replicas << replica.to_json
        end
        replicas
      end
    end

    class Replica
      def initialize(host, options={})
        @host       = host
        @port       = options[:port]     || DEFAULT_PORT
        @tag        = options[:tag]      || DEFAULT_TAG
        @n_slices   = options[:n_slices] || 1

        @n_volumes = 0
      end

      def to_json
        @json ||= generate_json
      end

      private
      def generate_json
        slices = []
        @n_slices.times do |index|
          slices << generate_slice
        end
        {
          "dimension" => "_key",
          "slicer" => "hash",
          "slices" => slices,
        }
      end

      def generate_slice
        name = sprintf('%03d', @n_volumes)
        @n_volumes += 1
        {
          "weight" => weight,
          "volume" => {
            "address" => "#{@host}:#{@port}/#{@tag}.#{name}",
          },
        }
      end

      def weight
        @weight ||= 100 / @n_slices
      end
    end
  end
end
