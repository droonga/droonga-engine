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
        unless event_for_me?
          puts " => ignoring event not for me"
          return true
        end

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
          puts "event sub name = #{@event_sub_name}"
        when "query"
          @event_sub_name = ENV["SERF_QUERY_NAME"]
          @payload = JSON.parse($stdin.gets)
          puts "event sub name = #{@event_sub_name}"
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
        when "publish_catalog"
          publish_catalog
        when "unpublish_catalog"
          unpublish_catalog
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
        puts "type = #{type}"
        case type
        when "replica"
          join_as_replica
        end
      end

      def join_as_replica
        source_node = @payload["source"]
        return unless source_node

        puts "source_node  = #{source_node}"

        source_host = source_node.split(":").first

        fetch_port = @payload["fetch_port"]
        catalog = fetch_catalog(source_node, fetch_port)
        generator = create_current_catalog_generator(catalog)
        dataset = generator.dataset_for_host(source_host) ||
                    generator.dataset_for_host(host)
        return unless dataset

        # restart self with the fetched catalog.
        SafeFileWriter.write(Path.catalog, JSON.pretty_generate(catalog))

        dataset_name = dataset.name
        tag          = dataset.replicas.tag
        port         = dataset.replicas.port
        other_hosts  = dataset.replicas.hosts

        puts "dataset = #{dataset_name}"
        puts "port    = #{port}"
        puts "tag     = #{tag}"

        if @payload["copy"]
          puts "starting to copy data from #{source_host}"

          modify_catalog do |modifier|
            modifier.datasets[dataset_name].replicas.hosts = [host]
          end
          sleep(1) # wait for restart

          DataAbsorber.absorb(:dataset          => dataset_name,
                              :source_host      => source_host,
                              :destination_host => host,
                              :port             => port,
                              :tag              => tag)
          sleep(1)
        end

        puts "joining to the cluster: update myself"

        modify_catalog do |modifier|
          modifier.datasets[dataset_name].replicas.hosts += other_hosts
          modifier.datasets[dataset_name].replicas.hosts.uniq!
        end
      end

      def fetch_catalog(source_node, port)
        source_host = source_node.split(":").first

        url = "http://#{source_host}:#{port}"
        connection = Faraday.new(url) do |builder|
          builder.response(:follow_redirects)
          builder.adapter(Faraday.default_adapter)
        end
        response = connection.get("/catalog.json")
        catalog = response.body

        JSON.parse(catalog)
      end

      def publish_catalog
        port = @payload["port"]
        return unless port

        env = {}
        publisher_command_line = [
          "droonga-engine-data-publisher",
            "--base-dir", Path.base.to_s,
            "--port", port.to_s,
            "--published-file", Path.catalog.to_s
        ]
        spawn(env, *publisher_command_line)
      end

      def unpublish_catalog
        port = @payload["port"]
        return unless port

        published_dir = Path.published(port)
        pid_file = published_dir + ".pid"

        Process.kill("INT", pid_file.read.to_i)
      end

      def set_replicas
        dataset = @payload["dataset"]
        return unless dataset

        hosts = given_hosts
        return unless hosts

        puts "new replicas: #{hosts.join(",")}"

        modify_catalog do |modifier|
          modifier.datasets[dataset].replicas.hosts = hosts
        end
      end

      def add_replicas
        dataset = @payload["dataset"]
        return unless dataset

        hosts = given_hosts
        return unless hosts

        hosts -= [host]
        return if hosts.empty?

        puts "adding replicas: #{hosts.join(",")}"

        modify_catalog do |modifier|
          modifier.datasets[dataset].replicas.hosts += hosts
          modifier.datasets[dataset].replicas.hosts.uniq!
        end
      end

      def remove_replicas
        dataset = @payload["dataset"]
        return unless dataset

        hosts = given_hosts
        return unless hosts

        puts "removing replicas: #{hosts.join(",")}"

        modify_catalog do |modifier|
          modifier.datasets[dataset].replicas.hosts -= hosts
        end
      end

      def modify_catalog
        generator = create_current_catalog_generator
        yield(generator)
        SafeFileWriter.write(Path.catalog, JSON.pretty_generate(generator.generate))
      end

      def create_current_catalog_generator(current_catalog=nil)
        current_catalog ||= JSON.parse(Path.catalog.read)
        generator = CatalogGenerator.new
        generator.load(current_catalog)
      end

      def absorb_data
        source = @payload["source"]
        return unless source

        puts "start to absorb data from #{source}"

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

        puts "dataset = #{dataset_name}"
        puts "port    = #{port}"
        puts "tag     = #{tag}"

        DataAbsorber.absorb(:dataset          => dataset_name,
                            :source_host      => source,
                            :destination_host => host,
                            :port             => port,
                            :tag              => tag)
      end

      def live_nodes
        nodes = {}
        raw_members = `#{@serf} members -rpc-addr #{@serf_rpc_address} -format json`
        members = JSON.parse(raw_members)
        members["members"].each do |member|
          if member["status"] == "alive"
            nodes[member["name"]] = {
              "serfAddress" => member["addr"],
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
