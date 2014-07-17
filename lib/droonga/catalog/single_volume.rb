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
    class SingleVolume
      def initialize(data)
        @data = data
        parse_address
      end

      def address
        @data["address"]
      end

      def host
        @host
      end

      def port
        @port
      end

      def tag
        @tag
      end

      def name
        @name
      end

      def node
        "#{host}:#{port}/#{tag}"
      end

      def all_nodes
        @all_nodes ||= [node]
      end

      private
      def parse_address
        if /\A(.+):(\d+)\/([^.]+)\.(.+)\z/ =~ address
          @host = $1
          @port = $2.to_i
          @tag = $3
          @name = $4
        else
          format = "${host_name}:${port_number}/${tag}.${name}"
          message = "volume address must be <#{format}> format: <#{address}>"
          raise ArgumentError, message
        end
      end
    end
  end
end
