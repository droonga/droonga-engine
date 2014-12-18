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
  class NodeStatus
    module Role
      SERVICE_PROVIDER   = "engine"
      ABSORB_SOURCE      = "engine-absorb-source"
      ABSORB_DESTINATION = "engine-absorb-destination"
    end

    def initialize
      reload
    end

    def have?(key)
      key = normalize_key(key)
      @status.include?(key)
    end

    def get(key)
      key = normalize_key(key)
      @status[key]
    end

    def set(key, value)
      key = normalize_key(key)
      @status[key] = value
      SafeFileWriter.write(status_file, JSON.pretty_generate(@status))
    end

    def delete(key)
      key = normalize_key(key)
      @status.delete(key)
      SafeFileWriter.write(status_file, JSON.pretty_generate(@status))
    end

    def role
      get(:role) || Role::SERVICE_PROVIDER
    end

    def role=(new_role)
      set(:role, new_role)
    end

    def reload
      @status = load
    end

    private
    def normalize_key(key)
      key.to_sym
    end

    def status_file
      @status_file ||= Path.node_status
    end

    def load
      if status_file.exist?
        contents = status_file.read
        unless contents.empty?
          return JSON.parse(contents, :symbolize_names => true)
        end
      end
      {}
    end
  end
end
