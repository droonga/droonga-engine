# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 droonga project
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

require 'digest/sha1'
require 'zlib'

module Droonga
  class << self
    def catalog
      @catalog ||= Catalog.new
    end
  end

  class Catalog
    CATALOG_FILE_PATH = 'catalog.json'

    def initialize
      open(CATALOG_FILE_PATH) do |file|
        @catalog = JSON.parse(file.read)
      end
      @catalog["datasets"].each do |name, dataset|
        number_of_partitions = dataset["number_of_partitions"]
        next if number_of_partitions < 2
        total_weight = dataset["ring"].reduce do |a, b|
          a[1]["weight"] + b[1]["weight"]
        end
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
    end

    def option(name)
      @catalog["options"][name]
    end

    def get_engines(name)
      device = @catalog["farms"][name]["device"]
      pattern = Regexp.new("^#{name}\.")
      results = {}
      @catalog["datasets"].each do |key, dataset|
        workers = dataset["workers"]
        plugins = dataset["plugins"]
        dataset["ring"].each do |key, part|
          part["partitions"].each do |range, partitions|
            partitions.each do |partition|
              if partition =~ pattern
                path = File.join([device, $POSTMATCH, 'db'])
                options = {
                  :database => path,
                  :n_workers => workers,
                  :handlers => plugins
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
      return routes unless dataset
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

    def dataset(name)
      @catalog["datasets"][name]
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
  end
end
