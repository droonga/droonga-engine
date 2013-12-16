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

module Droonga
  module MessagePackPacker
    class << self
      def to_msgpack(object)
        packer = MessagePack::Packer.new
        to_msgpack_internal(packer, object)
        packer.to_s
      end

      def to_msgpack_internal(packer, object)
        case object
        when Array
          packer.write_array_header(object.size)
          object.each do |element|
            to_msgpack_internal(packer, element)
          end
        when Hash
          packer.write_map_header(object.size)
          object.each do |key, value|
            to_msgpack_internal(packer, key)
            to_msgpack_internal(packer, value)
          end
        when Time
          packer.write(object.utc.iso8601)
        else
          packer.write(object)
        end
      end
    end
  end
end
