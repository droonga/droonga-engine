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

module Droonga
  module Catalog
    class Dataset
      def initialize(data)
        @data = data
      end

      def continuum
        @continuum ||= compute_continuum
      end

      private
      def compute_continuum
        number_of_partitions = @data["number_of_partitions"]
        return [] if number_of_partitions < 2
        total_weight = compute_total_weight
        continuum = []
        @data["ring"].each do |key, value|
          points = number_of_partitions * 160 * value["weight"] / total_weight
          points.times do |point|
            hash = Digest::SHA1.hexdigest("#{key}:#{point}")
            continuum << [hash[0..7].to_i(16), key]
          end
        end
        continuum.sort do |a, b|
          a[0] - b[0]
        end
      end

      def compute_total_weight
        @data["ring"].reduce(0) do |result, zone|
          result + zone[1]["weight"]
        end
      end
    end
  end
end
