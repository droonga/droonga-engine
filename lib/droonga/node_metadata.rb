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

require "json"
require "droonga/path"
require "droonga/safe_file_writer"

module Droonga
  class NodeMetadata
    module Role
      SERVICE_PROVIDER   = "service-provider"
      ABSORB_SOURCE      = "absorb-source"
      ABSORB_DESTINATION = "absorb-destination"
    end

    def initialize
      reload
    end

    def have?(key)
      key = normalize_key(key)
      @metadata.include?(key)
    end

    def get(key)
      key = normalize_key(key)
      @metadata[key]
    end

    def set(key, value)
      key = normalize_key(key)
      @metadata[key] = value
      SafeFileWriter.write(metadata_file, JSON.pretty_generate(@metadata))
    end

    def delete(key)
      key = normalize_key(key)
      @metadata.delete(key)
      SafeFileWriter.write(metadata_file, JSON.pretty_generate(@metadata))
    end

    def role
      get(:role) || Role::SERVICE_PROVIDER
    end

    def role=(new_role)
      set(:role, new_role)
    end

    def reload
      @metadata = load
    end

    private
    def normalize_key(key)
      key.to_sym
    end

    def metadata_file
      @metadata_file ||= Path.node_metadata
    end

    def load
      if metadata_file.exist?
        contents = metadata_file.read
        unless contents.empty?
          return JSON.parse(contents, :symbolize_names => true)
        end
      end
      {}
    end
  end
end
