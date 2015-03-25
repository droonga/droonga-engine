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

require "droonga/node_name"

module Droonga
  class Address < NodeName
    class << self
      def parse(string)
        if /\A(.+):(\d+)\/([^.]+)(?:\.(.+))?\z/ =~ string
          components = {
            :host => $1,
            :port => $2.to_i,
            :tag  => $3,
            :name => $4
          }
          new(components)
        else
          format = "${host_name}:${port_number}/${tag}.${name}"
          message = "volume address must be <#{format}> format: <#{string}>"
          raise ArgumentError, message
        end
      end
    end

    attr_reader :name
    def initialize(components={})
      super
      @name = components[:name]
    end

    def to_s
      string = super
      string << ".#{@name}" if @name
      string
    end

    def to_a
      super + [@name]
    end

    def ==(other)
      other.is_a?(self.class) and to_a == other.to_a
    end
  end
end
