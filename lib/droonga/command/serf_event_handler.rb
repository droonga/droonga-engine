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

require "json"

require "droonga/path"
require "droonga/serf"
require "droonga/catalog_generator"
require "droonga/data_absorber"
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
          @event_sub_name = ENV["SERF_USER_EVENT"]
          @payload = JSON.parse($stdin.gets)
        when "query"
          @event_sub_name = ENV["SERF_USER_QUERY"]
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
          join
        when "set_replicas"
          set_replicas
        when "add_replicas"
          add_replicas
        when "remove_replicas"
          remove_replicas
        when "absorb_data"
          absorb_data
        end
      end

      def host
        @serf_name.split(":").first
      end

      def given_hosts
        hosts = @payload["hosts"]
        return nil unless hosts
        hosts = [hosts] if hosts.is_a?(String)
        hosts
      end

      def join
        type = @payload["type"]
        case type
        when "replica"
          join_as_replica
        end
      end

      def join_as_replica
        source = @payload["source"]
        return unless source

        generator = create_current_catalog_generator
        dataset = generator.dataset_for_host(source)
        return unless dataset

        dataset_name = dataset.name
        tag          = dataset.tag
        port         = dataset.port
        other_hosts  = dataset.hosts

        if @payload["copy"]
          modify_catalog do |modifier|
            modifier.datasets[dataset].replicas.hosts = [host]
          end
          sleep(1) # wait for restart

          DataAbsorber.absorb(:dataset          => dataset,
                              :source_host      => source,
                              :destination_host => host,
                              :port             => port,
                              :tag              => tag)
          sleep(1)
        end

        modify_catalog do |modifier|
          modifier.datasets[dataset].replicas.hosts += other_hosts
          modifier.datasets[dataset].replicas.hosts.uniq!
        end
        sleep(1) # wait for restart

        source_node  = "#{source}:#{port}/#{tag}"
        Serf.send_query(source_node, "add_replicas",
                        "dataset" => dataset,
                        "hosts"   => [host])
      end

      def set_replicas
        dataset = @payload["dataset"]
        return unless dataset

        hosts = given_hosts
        return unless hosts

        modify_catalog do |modifier|
          modifier.datasets[dataset].replicas.hosts = hosts
        end
      end

      def add_replicas
        dataset = @payload["dataset"]
        return unless dataset

        hosts = given_hosts
        return unless hosts

        modify_catalog do |modifier|
          modifier.datasets[dataset].replicas.hosts += hosts
          modifier.datasets[dataset].replicas.hosts.uniq!
        end
      end

      def remove_replica
        dataset = @payload["dataset"]
        return unless dataset

        hosts = given_hosts
        return unless hosts

        modify_catalog do |modifier|
          modifier.datasets[dataset].replicas.hosts -= hosts
        end
      end

      def modify_catalog
        generator = create_current_catalog_generator
        yield(generator)
        SafeFileWriter.write(Path.catalog, JSON.pretty_generate(generator.catalog))
      end

      def create_current_catalog_generator
        current_catalog = JSON.parse(Path.catalog.read)
        generator = CatalogGenerator.new
        generator.load(current_catalog)
      end

      def absorb_data
        return unless event_for_me?

        source = @payload["source"]
        return unless source

        dataset_name = @payload["dataset"]
        port         = @payload["port"]
        tag          = @payload["tag"]

        if dataset_name.nil? or port.nil? or tag.nil?
          current_catalog = JSON.parse(Path.catalog.read)
          generator = CatalogGenerator.new
          generator.load(current_catalog)

          dataset = generator.dataset_for_host(source)
          return unless dataset

          dataset_name = dataset.name
          port = dataset.replicas.port
          tag  = dataset.replicas.tag
        end

        DataAbsorber.absorb(:dataset          => dataset,
                            :source_host      => source,
                            :destination_host => host,
                            :port             => port,
                            :tag              => tag)
        #TODO: how to notify that this process is successfully finished for other nodes?
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
