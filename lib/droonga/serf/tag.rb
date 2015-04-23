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
  class Serf
    module Tag
      class << self
        def node_type
          "type"
        end

        def node_role
          "role"
        end

        def internal_node_name
          "internal-name"
        end

        def cluster_id
          "cluster_id"
        end

        def accept_messages_newer_than
          "accept-newer-than"
        end

        def last_message_timestamp
          "last-timestamp"
        end

        HAVE_UNPROCESSED_MESSAGES_TAG_PREFIX = "buffered-for-"

        def have_unprocessed_messages_tag_for(node_name)
         "#{HAVE_UNPROCESSED_MESSAGES_TAG_PREFIX}#{node_name}"
        end

        def have_unprocessed_messages_tag?(tag)
          tag.start_with?(HAVE_UNPROCESSED_MESSAGES_TAG_PREFIX)
        end

        def extract_node_name_from_have_unprocessed_messages_tag(tag)
          tag.sub(HAVE_UNPROCESSED_MESSAGES_TAG_PREFIX, "")
        end
      end
    end
  end
end
