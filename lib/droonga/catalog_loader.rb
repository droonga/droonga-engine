# Copyright (C) 2013-2014 Droonga Project
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

require "json"

require "droonga/catalog/version1"
require "droonga/catalog/version2"

module Droonga
  class CatalogLoader
    def initialize(path)
      @path = path
    end

    def load
      data = nil
      begin
        data = File.open(@path) do |file|
          JSON.parse(file.read)
        end
      rescue Errno::ENOENT => error
        raise Error.new("Missing catalog file #{@path}")
      rescue JSON::ParserError => error
        raise Error.new("Syntax error in #{@path}:\n#{error.to_s}")
      end

      unless data.is_a?(Hash)
        raise Error.new("Root element of catalog must be an object in #{@path}")
      end

      version = data["version"]
      case version
      when 1
        Catalog::Version1.new(data, @path)
      when 2
        Catalog::Version2.new(data, @path)
      when nil
        raise Error.new("Catalog version must be specified in #{@path}")
      else
        raise Error.new("Unsupported catalog version <#{version}> is specified in #{@path}")
      end
    end
  end
end
