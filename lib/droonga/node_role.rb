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

require "json"

require "droonga/path"
require "droonga/serf/tag"

module Droonga
  class NodeRole
    SERVICE_PROVIDER   = "service-provider".downcase
    ABSORB_SOURCE      = "absorb-source".downcase
    ABSORB_DESTINATION = "absorb-destination".downcase

    ANY = "any".downcase

    #XXX ANY is not a valid role for a node. It is used
    #    just for checking acceptability of messages.
    ROLES = [
      SERVICE_PROVIDER,
      ABSORB_SOURCE,
      ABSORB_DESTINATION,
    ]

    class << self
      def normalize(role)
        new(role).to_s
      end

      def mine
        if Path.serf_tags_file.exist?
          tags = Path.serf_tags_file.read
          tags = JSON.parse(tags)
          role_from_tag = tags[Serf::Tag.node_role]
          return role_from_tag.downcase if role_from_tag
        end
        SERVICE_PROVIDER
      rescue Errno::ENOENT, JSON::ParserError
        SERVICE_PROVIDER
      end
    end

    def initialize(role)
      @role = normalize(role)
    end

    def to_s
      @role
    end

    private
    def valid?(role)
      ROLES.include?(role)
    end

    def normalize(role)
      role = role.to_s.downcase
      role = SERVICE_PROVIDER unless valid?(role)
      role
    end
  end
end
