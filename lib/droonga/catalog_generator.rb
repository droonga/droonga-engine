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
      catalog["datasets"].each do |name, dataset|
        add_dataset(name, dataset_to_params(dataset))
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
        return @options[:replicas] if @options[:replicas]
        @generated_replicas ||= Replicas.new(@options)
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

        @n_volumes = 0
      end

      def to_catalog
        slices = []
        @n_slices.times do
          slices << catalog_slice
        end
        {
          "dimension" => "_key",
          "slicer" => "hash",
          "slices" => slices,
        }
      end

      private
      def catalog_slice
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

    ADDRESS_MATCHER = /\A(.*):(\d+)\/([^\.]+)\.(.+)\z/

    def dataset_to_params(dataset)
      params = {}
      params[:n_workers] = dataset["nWorkers"]
      params[:n_slices]  = dataset["replicas"].first["slices"].size
      params[:plugins]   = dataset["plugins"]
      params[:schema]    = dataset["schema"] if dataset["schema"]
      params[:fact]      = dataset["fact"] if dataset["fact"]

      nodes = dataset["replicas"].collect do |replica|
        ADDRESS_MATCHER =~ replica["slices"].first["volume"]["address"]
        {
          :host => $1,
          :port => $2.to_i,
          :tag  => $3,
          :path => $4,
        }
      end
      params[:tag]   = nodes.first[:tag]
      params[:port]  = nodes.first[:port].to_i
      params[:hosts] = nodes.collect do |node|
        node[:host]
      end
      params
    end
  end
end
