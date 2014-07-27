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
require "droonga/catalog/collection_volume"

module Droonga
  module Catalog
    module Volume
      class << self
        def create(dataset, raw_volume)
          if raw_volume.key?("address")
            SingleVolume.new(raw_volume)
          else
            CollectionVolume.new(dataset, raw_volume)
          end
        end
      end
    end
  end
end
