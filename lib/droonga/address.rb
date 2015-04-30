# Copyright (C) 2014-2015 Droonga Project
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

require "droonga/node_name"

module Droonga
  class Address
    class << self
      def parse(string)
        if /\A(.+):(\d+)\/([^.]+)(?:\.(.+))?\z/ =~ string
          components = {
            :host => $1,
            :port => $2.to_i,
            :tag  => $3,
            :local_name => $4
          }
          new(components)
        else
          format = "${host_name}:${port_number}/${tag}.${local_name}"
          message = "volume address must be <#{format}> format: <#{string}>"
          raise ArgumentError, message
        end
      end
    end

    attr_reader :local_name
    def initialize(components={})
      @node_name  = NodeName.new(components)
      @local_name = components[:local_name]
    end

    def host
      @node_name.host
    end

    def port
      @node_name.port
    end

    def tag
      @node_name.tag
    end

    def node
      @node_name.node
    end

    def to_s
      string = @node_name.node
      string << ".#{@local_name}" if @local_name
      string
    end

    def to_a
      @node_name.to_a + [@local_name]
    end

    def ==(other)
      if other.is_a?(String)
        return to_s == other
      end
      other.is_a?(self.class) and to_a == other.to_a
    end
  end
end
