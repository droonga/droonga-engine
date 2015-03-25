# Copyright (C) 2015
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

require "socket"

module Droonga
  class NodeName
    class << self
      def parse(string)
        if /\A(.+):(\d+)\/([^.]+)\z/ =~ string
          components = {
            :host => $1,
            :port => $2.to_i,
            :tag  => $3,
          }
          new(components)
        else
          format = "${host_name}:${port_number}/${tag}"
          message = "node name must be <#{format}> format: <#{string}>"
          raise ArgumentError, message
        end
      end

      def valid?(string)
        begin
          parse(string)
          true
        rescue ArgumentError
          false
        end
      end
    end

    DEFAULT_HOST = Socket.gethostname
    DEFAULT_HOST.force_encoding("US-ASCII") if DEFAULT_HOST.ascii_only?
    DEFAULT_PORT = 10031
    DEFAULT_TAG  = "droonga"

    attr_reader :host
    attr_reader :port
    attr_reader :tag

    def initialize(components={})
      @host = components[:host] || DEFAULT_HOST
      @port = components[:port] || DEFAULT_PORT
      @tag  = components[:tag]  || DEFAULT_TAG
    end

    def to_s
      node
    end

    def node
      "#{@host}:#{@port}/#{@tag}"
    end

    def to_a
      [@host, @port, @tag]
    end

    def ==(other)
      other.is_a?(self.class) and to_a == other.to_a
    end
  end
end
