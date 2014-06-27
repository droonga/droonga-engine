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

require "optparse"
require "pathname"
require "json"
require "fileutils"
require "tempfile"

require "droonga/path"
require "droonga/serf"
require "droonga/catalog_generator"
require "droonga/safe_file_writer"

module Droonga
  module Command
    class SerfEventHandler
      class << self
        def run
          new.run
        end
      end

      def initialize
        @serf = ENV["SERF"] || Serf.path
        @serf_rpc_address = ENV["SERF_RPC_ADDRESS"] || "127.0.0.1:7373"
        @serf_name = ENV["SERF_SELF_NAME"]
      end

      def run
        parse_event
        return true unless event_for_me?

        process_event
        output_live_nodes
        true
      end

      private
      def parse_event
        @event_name = ENV["SERF_EVENT"]
        @payload = nil
        case @event_name
        when "user"
          @event_sub_name += ":#{ENV["SERF_USER_EVENT"]}"
          @payload = JSON.parse($stdin.gets)
        when "query"
          @event_sub_name += ":#{ENV["SERF_USER_QUERY"]}"
          @payload = JSON.parse($stdin.gets)
        end
      end

      def event_for_me?
        return true unless @payload
        return true unless @payload["node"]

        @payload["node"] == @serf_name
      end

      def process_event
        case @event_sub_name
        when "change_role"
          save_status(:role, @payload["role"])
        when "join"
          process_node_join
        end
      end

      def process_node_join
        dataset = @payload["dataset"]
        return unless dataset

        host = @payload["host"]
        return unless host

        return unless @payload["type"] == "replica"

        modifications = {
          dataset => {
            :add_replica_hosts => [host],
          },
        }
        modify_catalog(modifications)
      end

      def process_node_unjoin
        dataset = @payload["dataset"]
        return unless dataset

        host = @payload["host"]
        return unless host

        return unless @payload["type"] == "replica"

        modifications = {
          dataset => {
            :remove_replica_hosts => [host],
          },
        }
        modify_catalog(modifications)
      end

      def modify_catalog(modifications)
        current_catalog = JSON.parse(Path.catalog.read)
        current_params = CatalogGenerator.catalog_to_params(current_catalog)
        updated_params = CatalogGenerator.update_params(current_params,
                                                        modifications)
        updated_catalog = CatalogGenerator.generate(updated_params)
        SafeFileWriter.write(Catalog.path, JSON.pretty_generate(updated_catalog))
      end

      def live_nodes
        nodes = {}
        members = `#{@serf} members -rpc-addr #{@serf_rpc_address}`
        members.each_line do |member|
          name, address, status, = member.strip.split(/\s+/)
          if status == "alive"
            nodes[name] = {
              "serfAddress" => address,
            }
          end
        end
        nodes
      end

      def output_live_nodes
        path = Path.live_nodes
        nodes = live_nodes
        file_contents = JSON.pretty_generate(nodes)
        SafeFileWriter.write(path, file_contents)
      end

      def save_status(key, value)
        status = Serf.load_status
        status[key] = value
        SafeFileWriter.write(Serf.status_file, JSON.pretty_generate(status))
      end
    end
  end
end
