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

require "time"

require "droonga/catalog/dataset"

module Droonga
  class CatalogGenerator
    DEFAULT_DATASET = "Default"
    DEFAULT_HOSTS = ["127.0.0.1"]
    DEFAULT_N_WORKERS = 4
    DEFAULT_N_SLICES = 1
    DEFAULT_PLUGINS = ["groonga", "search", "crud", "dump", "system"]
    DEFAULT_PORT = 10031
    DEFAULT_TAG = "droonga"

    attr_reader :datasets

    class << self
      def generate(datasets_params)
        generator = new
        datasets_params.each do |name, params|
          generator.add_dataset(name, params)
        end
        generator.generate
      end
    end

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

    def load(catalog)
      catalog["datasets"].each do |name, catalog_dataset|
        load_dataset(name, catalog_dataset)
      end
      self
    end

    def dataset_for_host(host)
      @datasets.each do |name, dataset|
        if dataset.replicas.hosts.include?(host)
          return dataset
        end
      end
      nil
    end

    def modify(dataset_modifications)
      dataset_modifications.each do |name, modification|
        dataset = @datasets[name]
        next unless dataset

        replicas = dataset.replicas

        if modification[:replica_hosts]
          replicas.hosts = modification[:replica_hosts]
        end

        if modification[:add_replica_hosts]
          replicas.hosts += modification[:add_replica_hosts]
          replicas.hosts.uniq!
        end

        if modification[:remove_replica_hosts]
          replicas.hosts -= modification[:remove_replica_hosts]
        end
      end
    end

    private
    def catalog_datasets
      catalog_datasets = {}
      @datasets.each do |name, dataset|
        catalog_datasets[name] = dataset.to_catalog
      end
      catalog_datasets
    end

    def load_dataset(name, catalog_dataset)
      options = {}
      options[:n_workers] = catalog_dataset["nWorkers"]
      options[:plugins]   = catalog_dataset["plugins"]
      options[:schema]    = catalog_dataset["schema"]
      options[:fact]      = catalog_dataset["fact"]
      options[:replicas]  = catalog_dataset["replicas"]
      add_dataset(name, options)
    end

    class Dataset
      attr_reader :name

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
        @replicas ||= create_replicas
      end

      def to_catalog
        catalog = {
          "nWorkers" => n_workers,
          "plugins"  => plugins,
          "schema"   => schema,
          "replicas" => replicas.to_catalog,
        }
        catalog["fact"] = fact if fact
        catalog
      end

      private
      def create_replicas
        catalog_replicas = @options[:replicas]
        if catalog_replicas
          replicas = Replicas.new
          replicas.load(catalog_replicas)
          replicas
        else
          Replicas.new(@options)
        end
      end
    end

    class Replicas
      attr_accessor :hosts
      attr_reader :port, :tag, :n_slices

      def initialize(options={})
        @hosts      = options[:hosts] || DEFAULT_HOSTS
        @port       = options[:port]
        @tag        = options[:tag]
        @n_slices   = options[:n_slices]
      end

      def load(catalog_replicas)
        dataset = Catalog::Dataset.new("temporary",
                                       "replicas" => catalog_replicas)
        @hosts = dataset.replicas.collect do |replica|
          replica.slices.first.volume.address.host
        end
        collection_volume = dataset.replicas.first
        slices = collection_volume.slices
        @n_slices = slices.size
        single_volume_address = slices.first.volume.address
        @port = single_volume_address.port
        @tag = single_volume_address.tag
      end

      def to_catalog
        catalog_replicas = []
        @hosts.each do |host|
          replica = Replica.new(host, :port => @port,
                                      :tag => @tag,
                                      :n_slices => @n_slices)
          catalog_replicas << replica.to_catalog
        end
        catalog_replicas
      end
    end

    class Replica
      def initialize(host, options={})
        @host       = host
        @port       = options[:port]     || DEFAULT_PORT
        @tag        = options[:tag]      || DEFAULT_TAG
        @n_slices   = options[:n_slices] || DEFAULT_N_SLICES
      end

      def to_catalog
        slices = []
        @n_slices.times do |i|
          slices << catalog_slice(i)
        end
        {
          "dimension" => "_key",
          "slicer" => "hash",
          "slices" => slices,
        }
      end

      private
      def catalog_slice(nth_slice)
        name = "%03d" % nth_slice
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
