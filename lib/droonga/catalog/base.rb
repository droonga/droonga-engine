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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "digest/sha1"
require "zlib"
require "time"
require "digest"
require "droonga/error_messages"
require "droonga/catalog/errors"

module Droonga
  module Catalog
    class Base
      attr_reader :path, :base_path
      def initialize(data, path=nil)
        @data = data
        @path = path || "/tmp/temporary-catalog.json"
        @base_path = File.dirname(path)
      end

      def have_dataset?(name)
        datasets.key?(name)
      end

      def dataset(name)
        datasets[name]
      end

      def cluster_id
        @cluster_id ||= calculate_cluster_id
      end

      private
      def calculate_cluster_id
        raw_id = []
        datasets.each do |name, dataset|
          raw_id << "#{name}-#{dataset.all_nodes.sort.join(",")}"
        end
        Digest::SHA1.hexdigest(raw_id.sort.join("|"))
      end

      def migrate_database_location(current_db_path, device, name)
        return if current_db_path.exist?

        common_base_path = Pathname(@base_path)
        old_db_paths = {
          :top_level     => common_base_path + device + name + "db",
          :singular_form => common_base_path + device + "database" + name + "db",
        }
        old_db_paths.each do |type, old_db_path|
          if old_db_path.exist?
            FileUtils.mkdir_p(current_db_path.parent.parent)
            FileUtils.move(old_db_path.parent, current_db_path.parent)
            return
          end
        end
      end
    end
  end
end
