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

require "droonga/catalog/single_volume"
require "droonga/catalog/slices_volume"
require "droonga/catalog/replicas_volume"

module Droonga
  module Catalog
    module Volume
      class UnknownTypeVolume < ArgumentError
        def initialize(raw_volume)
          super("volume must have one of 'address', 'slices' or 'replicas': " +
                  "#{raw_volume.inspect}")
        end
      end

      class << self
        def create(dataset, raw_volume)
          if raw_volume.key?("address")
            SingleVolume.new(raw_volume)
          elsif raw_volume.key?("slices")
            SlicesVolume.new(dataset, raw_volume)
          elsif raw_volume.key?("replicas")
            ReplicasVolume.new(dataset, raw_volume)
          else
            raise UnknownTypeVolume.new(raw_volume)
          end
        end
      end
    end
  end
end
