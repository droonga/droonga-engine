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

require "digest/sha1"
require "zlib"
require "time"
require "droonga/error_messages"
require "droonga/catalog/errors"
require "droonga/catalog/live_nodes_list"

module Droonga
  module Catalog
    class Base
      attr_reader :path, :base_path
      def initialize(data, path, options={})
        @data = data
        @path = path
        @options = options
        @base_path = File.dirname(path)
      end

      def have_dataset?(name)
        datasets.key?(name)
      end

      def dataset(name)
        datasets[name]
      end

      def live_nodes_list
        @live_nodes_list ||= LiveNodesList.new(@options[:live_nodes_file])
      end
    end
  end
end
