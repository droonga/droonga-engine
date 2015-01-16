# Copyright (C) 2014-2015 Droonga Project
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
require "droonga/loggable"
require "droonga/catalog_loader"
require "droonga/node_metadata"
require "droonga/serf/downloader"
require "droonga/serf/agent"
require "droonga/serf/command"
require "droonga/line_buffer"
require "droonga/safe_file_writer"
require "droonga/service_installation"

module Droonga
  class Serf
    class << self
      def path
        Droonga::Path.base + "serf"
      end
    end

    include Loggable

    def initialize(name)
      @serf = nil
      @name = name
      @service_installation = ServiceInstallation.new
      @node_metadata = NodeMetadata.new
    end

    def run_agent(loop)
      logger.trace("run_agent: start")
      ensure_serf
      retry_joins = []
      detect_other_hosts.each do |other_host|
        retry_joins.push("-retry-join", other_host)
      end
      agent = Agent.new(loop, @serf,
                        extract_host(@name), agent_port, rpc_port,
                        "-node", @name,
                        "-event-handler", "droonga-engine-serf-event-handler",
                        "-tag", "type=engine",
                        "-tag", "role=#{role}",
                        "-tag", "cluster_id=#{cluster_id}",
                        *retry_joins)
      agent.on_ready = lambda do
        update_cluster_state
      end
      agent.start
      logger.trace("run_agent: done")
      agent
    end

    def leave
      run_command("leave")
    end

    def join(*hosts)
      nodes = hosts.collect do |host|
        "#{host}:#{agent_port}"
      end
      run_command("join", *nodes)
    end

    def send_query(query, payload)
      options = ["-format", "json"] + additional_options_from_payload(payload)
      options += [query, JSON.generate(payload)]
      raw_serf_response = run_command("query", *options)
      serf_response = JSON.parse(raw_serf_response)

      node = payload["node"]
      if node
        responses = serf_response["Responses"]
        response = responses[node]
        if response.is_a?(String)
          begin
            JSON.parse(response)
          rescue JSON::ParserError
            response
          end
        else
          response
        end
      else
        response
      end
    end

    def update_cluster_state
      path = Path.cluster_state
      new_state = current_cluster_state
      file_contents = JSON.pretty_generate(new_state)
      SafeFileWriter.write(path) do |output, file|
        output.puts(file_contents)
        @service_installation.ensure_correct_file_permission(file)
      end
    end

    def current_cluster_state
      raw_response = run_command("members", "-format", "json")
      response = JSON.parse(raw_response)

      current_cluster_id = cluster_id
      nodes = {}
      response["members"].each do |member|
        foreign = member["tags"]["cluster_id"] != current_cluster_id
        next if foreign

        nodes[member["name"]] = {
          "type" => member["tags"]["type"],
          "role" => member["tags"]["role"],
          "live" => member["status"] == "alive",
        }
      end
      nodes
    end

    def set_tag(name, value)
      run_command("tags", "-set", "#{name}=#{value}")
    end

    def delete_tag(name)
      run_command("tags", "-delete", name)
    end

    def update_cluster_id
      set_tag("cluster_id", cluster_id)
    end

    def role
      @node_metadata.role
    end

    def role=(new_role)
      new_role ||= NodeMetadata::Role::SERVICE_PROVIDER
      set_tag("role", new_role)
      @node_metadata.role = new_role
    end

    def cluster_id
      loader = CatalogLoader.new(Path.catalog.to_s)
      catalog = loader.load
      catalog.cluster_id
    end

    private
    def ensure_serf
      @serf ||= find_system_serf
      return if @serf

      serf_path = self.class.path
      @serf = serf_path.to_s
      return if serf_path.executable?
      downloader = Downloader.new(serf_path)
      downloader.download
    end

    def find_system_serf
      paths = (ENV["PATH"] || "").split(File::PATH_SEPARATOR)
      paths.each do |path|
        serf = File.join(path, "serf")
        return serf if File.executable?(serf)
      end
      nil
    end

    def run_command(command, *options)
      ensure_serf
      command = Command.new(@serf, command,
                            "-rpc-addr", rpc_address,
                            *options)
      command.run
    end

    def additional_options_from_payload(payload)
      options = []
      if payload.is_a?(Hash) and payload.include?("node")
        options += ["-node", payload["node"]]
      end
      options
    end

    def extract_host(node_name)
      node_name.split(":").first
    end

    def rpc_address
      "#{extract_host(@name)}:#{rpc_port}"
    end

    def rpc_port
      7373
    end

    def agent_port
      Agent::PORT
    end

    def detect_other_hosts
      loader = CatalogLoader.new(Path.catalog.to_s)
      catalog = loader.load
      other_nodes = catalog.all_nodes.reject do |node|
        node == @name
      end
      other_nodes.collect do |node|
        extract_host(node)
      end
    end

    def log_tag
      "serf"
    end
  end
end
