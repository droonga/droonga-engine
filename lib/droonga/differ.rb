# Copyright (C) 2015 Droonga Project
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

module Droonga
  module Differ
    class << self
      def diff(a, b)
        unless a.class == b.class
          return "#{a.inspect} <=> #{b.inspect}"
        end
        case a
        when Hash
          diff_hashes(a, b)
        when Array
          diff_arrays(a, b)
        else
          if a == b
            nil
          else
            "#{a.inspect} <=> #{b.inspect}"
          end
        end
      end

      def diff_hashes(a, b)
        difference = {}
        (a.keys + b.keys).uniq.each do |key|
          unless a[key] == b[key]
            difference[key] = diff(a[key], b[key])
          end
        end
        difference
      end

      def diff_arrays(a, b)
        difference = {}
        [a.size, b.size].max.times do |index|
          unless a[index] == b[index]
            difference[index] = diff(a[index], b[index])
          end
        end
        difference
      end
    end
  end
end
