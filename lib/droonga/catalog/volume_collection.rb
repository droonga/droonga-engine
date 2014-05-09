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
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

module Droonga
  module Catalog
    class VolumeCollection
      include Enumerable

      def initialize(volumes)
        @volumes = volumes
      end

      def each(&block)
        @volumes.each(&block)
      end

      def ==(other)
        other.is_a?(self.class) and
          to_a == other.to_a
      end

      def eql?(other)
        self == other
      end

      def hash
        to_a.hash
      end

      def select(how=nil)
        case how
        when :top
          [@volumes.first]
        when :random
          [@volumes.sample]
        when :all
          @volumes
        else
          super
        end
      end

      def all_nodes
        nodes = []
        @volumes.each do |volume|
          nodes += volume.all_nodes
        end
        nodes.sort.uniq
      end
    end
  end
end
