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
  class CollectorMessage
    attr_reader :raw
    def initialize(raw)
      @raw = raw
    end

    def valid?
      task and step and values
    end

    def [](key)
      @raw[key]
    end

    def task
      @raw["task"]
    end

    def step
      task["step"]
    end

    def type
      step["type"]
    end

    def values
      task["values"]
    end

    def body
      step["body"]
    end

    def input
      if body
        body[name]
      else
        nil
      end
    end

    def outputs
      step["outputs"]
    end

    def name
      @raw["name"]
    end

    def value
      @raw["value"]
    end
  end
end
