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

require "msgpack"

module Droonga
  class MessagePackPacker
    class << self
      def pack(object)
        packer = new
        packer.pack(object)
        packer.to_s
      end
    end

    def initialize
      @packer = MessagePack::Packer.new
    end

    def pack(object)
      case object
      when Array
        @packer.write_array_header(object.size)
        object.each do |element|
          pack(element)
        end
      when Hash
        @packer.write_map_header(object.size)
        object.each do |key, value|
          pack(key)
          pack(value)
        end
      when Time
        @packer.write(object.utc.iso8601)
      else
        @packer.write(object)
      end
    end

    def to_s
      @packer.to_s
    end
  end
end
