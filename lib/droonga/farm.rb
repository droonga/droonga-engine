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

require "droonga/slice"

module Droonga
  class Farm
    def initialize(name, catalog, loop, options={})
      @name = name
      @catalog = catalog
      @loop = loop
      @options = options
      @slices = {}
      slices = @catalog.slices(name)
      slices.each do |slice_name, slice_options|
        slice = Droonga::Slice.new(@loop,
                                   @options.merge(slice_options))
        @slices[slice_name] = slice
      end
    end

    def start
      @slices.each_value do |slice|
        slice.start
      end
    end

    def shutdown
      @slices.each_value do |slice|
        slice.shutdown
      end
    end

    def process(slice_name, message)
      @slices[slice_name].process(message)
    end
  end
end
