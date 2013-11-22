# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Droonga Project
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

require "droonga/partition"

module Droonga
  class Farm
    def initialize(name)
      @name = name
      @partitions = {}
      Droonga.catalog.get_partitions(name).each do |partition_name, options|
        partition = Droonga::Partition.new(options)
        @partitions[partition_name] = partition
      end
    end

    def start
      @partitions.each_value do |partition|
        partition.start
      end
    end

    def shutdown
      @partitions.each_value do |partition|
        partition.shutdown
      end
    end

    # TODO: fix method name
    def emit(partition_name, envelope, synchronous)
      @partitions[partition_name].emit('', Time.now.to_f, envelope, synchronous)
    end
  end
end
