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

require "digest/sha1"
require "zlib"
require "time"
require "droonga/message_processing_error"

module Droonga
  module Catalog
    class ValidationError < Error
      def initialize(message, path)
        if path
          super("Validation error in #{path}: #{message}")
        else
          super(message)
        end
      end
    end

    class MissingRequiredParameter < ValidationError
      def initialize(name, path)
        super("You must specify \"#{name}\".", path)
      end
    end

    class MismatchedParameterType < ValidationError
      def initialize(name, expected, actual, path)
        super("\"#{name}\" must be a #{expected}, but a #{actual}.", path)
      end
    end

    class InvalidDate < ValidationError
      def initialize(name, value, path)
        super("\"#{name}\" must be a valid datetime. " +
                "\"#{value}\" cannot be parsed as a datetime.", path)
      end
    end

    class NegativeNumber < ValidationError
      def initialize(name, actual, path)
        super("\"#{name}\" must be a positive number, but #{actual}.", path)
      end
    end

    class SmallerThanOne < ValidationError
      def initialize(name, actual, path)
        super("\"#{name}\" must be 1 or larger number, but #{actual}.", path)
      end
    end

    class Base
      attr_reader :path, :base_path
      def initialize(data, path)
        @data = data
        @path = path
        @base_path = File.dirname(path)

        validate

        @data["datasets"].each do |name, dataset|
          number_of_partitions = dataset["number_of_partitions"]
          next if number_of_partitions < 2
          total_weight = compute_total_weight(dataset)
          continuum = []
          dataset["ring"].each do |key, value|
            points = number_of_partitions * 160 * value["weight"] / total_weight
            points.times do |point|
              hash = Digest::SHA1.hexdigest("#{key}:#{point}")
              continuum << [hash[0..7].to_i(16), key]
            end
          end
          dataset["continuum"] = continuum.sort do |a, b| a[0] - b[0]; end
        end
        @options = @data["options"] || {}
      end

      def option(name)
        @options[name]
      end

      def get_partitions(name)
        device = @data["farms"][name]["device"]
        pattern = Regexp.new("^#{name}\.")
        results = {}
        @data["datasets"].each do |key, dataset|
          workers = dataset["workers"]
          plugins = dataset["plugins"]
          dataset["ring"].each do |key, part|
            part["partitions"].each do |range, partitions|
              partitions.each do |partition|
                if partition =~ pattern
                  path = File.join([device, $POSTMATCH, "db"])
                  path = File.expand_path(path, base_path)
                  options = {
                    :database => path,
                    :n_workers => workers,
                    :plugins => plugins
                  }
                  results[partition] = options
                end
              end
            end
          end
        end
        return results
      end

      def get_routes(name, args)
        routes = []
        dataset = dataset(name)
        case args["type"]
        when "broadcast"
          dataset["ring"].each do |key, partition|
            select_range_and_replicas(partition, args, routes)
          end
        when "scatter"
          name = get_partition(dataset, args["key"])
          partition = dataset["ring"][name]
          select_range_and_replicas(partition, args, routes)
        end
        return routes
      end

      def get_partition(dataset, key)
        continuum = dataset["continuum"]
        return dataset["ring"].keys[0] unless continuum
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
        return continuum[max][1]
      end

      def datasets
        @data["datasets"] || {}
      end

      def have_dataset?(name)
        datasets.key?(name)
      end

      def dataset(name)
        datasets[name]
      end

      def select_range_and_replicas(partition, args, routes)
        date_range = args["date_range"] || 0..-1
        partition["partitions"].sort[date_range].each do |time, replicas|
          case args["replica"]
          when "top"
            routes << replicas[0]
          when "random"
            routes << replicas[rand(replicas.size)]
          when "all"
            routes.concat(replicas)
          end
        end
      end

      private
      def compute_total_weight(dataset)
        dataset["ring"].reduce(0) do |result, zone|
          result + zone[1]["weight"]
        end
      end

      def validate
        validate_effective_date
        validate_zones
        validate_farms
        validate_datasets
      end

      def validate_parameter_type(expected, value, name)
        unless value.is_a?(expected)
          raise MismatchedParameterType.new(name,
                                            expected,
                                            value.class,
                                            @path)
        end
      end

      def validate_valid_datetime(value, name)
        validate_parameter_type(String, value, name)
        begin
          Time.parse(value)
        rescue ArgumentError => error
          raise InvalidDate.new(name, value, @path)
        end
      end

      def validate_positive_numeric_parameter(value, name)
        validate_parameter_type(Numeric, value, name)
        if value < 0
          raise NegativeNumber.new(name, value, @path)
        end
      end

      def validate_positive_integer_parameter(value, name)
        validate_parameter_type(Integer, value, name)
        if value < 0
          raise NegativeNumber.new(name, value, @path)
        end
      end

      def validate_one_or_larger_integer_parameter(value, name)
        validate_parameter_type(Integer, value, name)
        if value < 1
          raise SmallerThanOne.new(name, value, @path)
        end
      end

      def validate_effective_date
        validate_valid_datetime(@data["effective_date"], "effective_date")
      end

      def validate_zones
        zones = @data["zones"]

        raise MissingRequiredParameter.new("zones", @path) unless zones
        validate_parameter_type(Array, zones, "zones")

        zones.each_with_index do |value, index|
          validate_parameter_type(String, value, "zones[#{index}]")
        end
      end

      def validate_farms
        farms = @data["farms"]

        raise MissingRequiredParameter.new("farms", @path) unless farms
        validate_parameter_type(Hash, farms, "farms")

        farms.each do |key, value|
          validate_farm(value, "farms.#{key}")
        end
      end

      def validate_farm(farm, name)
        validate_parameter_type(Hash, farm, name)
        validate_parameter_type(String, farm["device"], "#{name}.device")
      end

      def validate_datasets
        datasets = @data["datasets"]

        raise MissingRequiredParameter.new("datasets", @path) unless datasets
        validate_parameter_type(Hash, datasets, "datasets")

        datasets.each do |name, dataset|
          validate_dataset(dataset, "datasets.#{name}")
        end
      end

      def validate_dataset(dataset, name)
        validate_parameter_type(Hash, dataset, name)

        validate_one_or_larger_integer_parameter(dataset["number_of_partitions"],
                                                 "#{name}.number_of_partitions")
        validate_one_or_larger_integer_parameter(dataset["number_of_replicas"],
                                                 "#{name}.number_of_replicas")
        validate_positive_integer_parameter(dataset["workers"],
                                            "#{name}.workers")
        validate_date_range(dataset["date_range"], "#{name}.date_range")

        validate_parameter_type(Hash, dataset["ring"], "#{name}.ring")
        dataset["ring"].each do |key, value|
          validate_ring(value, "#{name}.ring.#{key}")
        end

        validate_parameter_type(Array, dataset["plugins"], "#{name}.plugins")
      end

      def validate_date_range(value, name)
        return if value == "infinity"
      end

      def validate_ring(ring, name)
        validate_parameter_type(Hash, ring, name)

        validate_positive_numeric_parameter(ring["weight"], "#{name}.weight")

        validate_parameter_type(Hash, ring["partitions"], "#{name}.partitions")
        ring["partitions"].each do |key, value|
          validate_partition(value, "#{name}.partitions.#{key}")
        end
      end

      def validate_partition(partition, name)
        validate_parameter_type(Array, partition, name)

        partition.each_with_index do |value, index|
          validate_parameter_type(String, value, "#{name}[#{index}]")
        end
      end
    end
  end
end
