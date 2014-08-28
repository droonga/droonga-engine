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
require "droonga/catalog_generator"
require "droonga/safe_file_writer"

module Droonga
  class CatalogModifier
    class << self
      def modify
        new.modify do |generator|
          yield(generator)
        end
      end
    end

    def initialize
      @generator = CatalogGenerator.new
      @catalog = JSON.parse(Path.catalog.read)
      @generator.load(@catalog)
    end

    def modify
      yield(@generator)
      SafeFileWriter.write(Path.catalog, JSON.pretty_generate(@generator.generate))
    end
  end
end
