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
require "droonga/serf"
require "droonga/catalog_generator"
require "droonga/data_absorber"
require "droonga/safe_file_writer"
require "droonga/client"

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
        @response = {
          "log" => []
        }
      end

      def run
        parse_event
        unless event_for_me?
          log(" => ignoring event not for me")
          output_response
          return true
        end

        process_event
        output_live_nodes
        output_response
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
          log("event sub name = #{@event_sub_name}")
        when "query"
          @event_sub_name = ENV["SERF_QUERY_NAME"]
          @payload = JSON.parse($stdin.gets)
          log("event sub name = #{@event_sub_name}")
        when "member-join", "member-leave", "member-update", "member-reap"
          output_live_nodes
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
        when "report_status"
          report_status
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

      def output_response
        puts JSON.generate(@response)
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

      def report_status
        @response["value"] = status(@payload["key"].to_sym)
      end

      def join
        type = @payload["type"]
        log("type = #{type}")
        case type
        when "replica"
          join_as_replica
        end
      end

      def join_as_replica
        source_node         = @payload["source"]
        source_node_port    = @payload["port"]
        source_node_dataset = @payload["dataset"]
        joining_node        = @payload["node"]
        tag                 = @payload["tag"]
        dataset             = @payload["dataset"]
        required_params = [
          source_node,
          source_node_port,
          source_node_dataset,
          joining_node,
          dataset,
        ]
        return unless required_params.all?

        log("source_node  = #{source_node}")

        source_host  = source_node.split(":").first
        joining_host = joining_node.split(":").first

        catalog = nil
        Droonga::Client.open(:host          => source_host,
                             :port          => source_node_port,
                             :tag           => tag,
                             :protocol      => :droonga,
                             :timeout       => 1,
                             :receiver_host => joining_host,
                             :receiver_port => 0) do |client|
          request = client.request(:dataset => source_node_dataset , 
                                   :type    => "catalog.fetch") do |responce|
            File.write(Path.catalog, JSON.generate(responce["body"]))
            catalog = responce["body"]
          end
          request.wait
        end

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

        log("dataset = #{dataset_name}")
        log("port    = #{port}")
        log("tag     = #{tag}")

        if @payload["copy"]
          log("starting to copy data from #{source_host}")

          modify_catalog do |modifier|
            modifier.datasets[dataset_name].replicas.hosts = [host]
          end
          sleep(5) #TODO: wait for restart. this should be done more safely, to avoid starting of absorbing with old catalog.json.

          save_status(:absorbing, true)
          DataAbsorber.absorb(:dataset          => dataset_name,
                              :source_host      => source_host,
                              :destination_host => host,
                              :port             => port,
                              :tag              => tag)
          delete_status(:absorbing)
          sleep(1)
        end

        log("joining to the cluster: update myself")

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
        pid = spawn(env, *publisher_command_line)
        Process.detach(pid)
        sleep(1) # wait until the directory is published

        published_dir = Path.published(port)
        pid_file = published_dir + ".pid"

        File.open(pid_file.to_s, "w") do |file|
          file.puts(pid)
        end
      end

      def unpublish_catalog
        port = @payload["port"]
        return unless port

        published_dir = Path.published(port)
        pid_file = published_dir + ".pid"
        pid = pid_file.read.to_i

        Process.kill("INT", pid)
      end

      def set_replicas
        dataset = @payload["dataset"]
        return unless dataset

        hosts = given_hosts
        return unless hosts

        log("new replicas: #{hosts.join(",")}")

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

        log("adding replicas: #{hosts.join(",")}")

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

        log("removing replicas: #{hosts.join(",")}")

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

        log("start to absorb data from #{source}")

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

        log("dataset = #{dataset_name}")
        log("port    = #{port}")
        log("tag     = #{tag}")

        save_status(:absorbing, true)
        DataAbsorber.absorb(:dataset          => dataset_name,
                            :source_host      => source,
                            :destination_host => host,
                            :port             => port,
                            :tag              => tag,
                            :client           => "droonga-send")
        delete_status(:absorbing)
      end

      def live_nodes
        Serf.live_nodes(@serf_name)
      end

      def output_live_nodes
        path = Path.live_nodes
        nodes = live_nodes
        file_contents = JSON.pretty_generate(nodes)
        SafeFileWriter.write(path, file_contents)
      end

      def status(key)
        Serf.status(key)
      end

      def save_status(key, value)
        status = Serf.load_status
        status[key] = value
        SafeFileWriter.write(Serf.status_file, JSON.pretty_generate(status))
      end

      def delete_status(key)
        status = Serf.load_status
        status.delete(key)
        SafeFileWriter.write(Serf.status_file, JSON.pretty_generate(status))
      end

      def log(message)
        @response["log"] << message
      end
    end
  end
end
