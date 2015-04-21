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

require "droonga/serf"

module Droonga
  class NodeRole
    SERVICE_PROVIDER   = "service-provider"
    ABSORB_SOURCE      = "absorb-source"
    ABSORB_DESTINATION = "absorb-destination"

    ROLES = [
      SERVICE_PROVIDER,
      ABSORB_SOURCE,
      ABSORB_DESTINATION,
    ]

    class << self
      def valid?(role)
        ROLES.include?(role)
      end
    end

    def initialize(role)
      @role = normalize(role)
    end

    def to_s
      @role
    end

    private
    def normalize(role)
      role = SERVICE_PROVIDER unless self.class.valid?(role)
      role
    end
  end
end
