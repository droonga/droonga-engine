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
require "droonga/catalog/loader"
require "droonga/node_name"
require "droonga/node_role"
require "droonga/serf/tag"
require "droonga/serf/downloader"
require "droonga/serf/agent"
require "droonga/serf/command"
require "droonga/line_buffer"
require "droonga/safe_file_writer"
require "droonga/service_installation"
require "droonga/restarter"

module Droonga
  class Serf
    include Loggable

    def initialize(name, options={})
      @serf_command = nil
      @name = NodeName.parse(name)
      @verbose = options[:verbose] || false
      @service_installation = ServiceInstallation.new
      @tags_cache = {}
    end

    def run_agent(loop)
      logger.trace("run_agent: start")
      ensure_serf
      retry_joins = []
      detect_other_hosts.each do |other_host|
        retry_joins.push("-retry-join", other_host)
      end
      tags_file = Path.serf_tags_file
      FileUtils.mkdir_p(tags_file.dirname)
      agent = Agent.new(loop, @serf_command,
                        @name.host, agent_port, rpc_port,
                        "-node", @name.to_s,
                        "-event-handler", "droonga-engine-serf-event-handler",
                        "-tags-file", tags_file.to_s,
                        *retry_joins)
      agent.start
      logger.trace("run_agent: done")
      agent
    end

    def initialize_tags
      set_tag(Tag.node_type, "engine")
      set_tag(Tag.node_role, role)
      set_tag(Tag.cluster_id, cluster_id)
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

    def current_members
      raw_response = run_command("members", "-format", "json")
      response = JSON.parse(raw_response)
      response["members"]
    end

    def current_cluster_state
      current_cluster_id = cluster_id
      nodes = {}
      unprocessed_messages_existence = {}
      current_members.each do |member|
        foreign = member["tags"][Tag.cluster_id] != current_cluster_id
        next if foreign

        member["tags"].each do |key, value|
          next unless Tag.have_unprocessed_messages_tag?(key)
          node_name = Tag.extract_node_name_from_have_unprocessed_messages_tag(key)
          next if unprocessed_messages_existence[node_name]
          unprocessed_messages_existence[node_name] = value == "true"
        end

        nodes[member["name"]] = {
          "type" => member["tags"][Tag.node_type],
          "role" => member["tags"][Tag.node_role],
          "internal_name" => member["tags"][Tag.internal_node_name],
          "accept_messages_newer_than" => member["tags"][Tag.accept_messages_newer_than],
          "live" => member["status"] == "alive",
        }
      end
      unprocessed_messages_existence.each do |node_name, have_messages|
        nodes[node_name]["have_unprocessed_messages"] = have_messages
      end
      sorted_nodes = {}
      nodes.keys.sort.each do |key|
        sorted_nodes[key] = nodes[key]
      end
      sorted_nodes
    end

    def get_tag(name)
      myself = current_members.find do |member|
        member["name"] == @name.to_s
      end
      if myself
        myself["tags"][name]
      else
        nil
      end
    end

    def set_tag(name, value)
      run_command("tags", "-set", "#{name}=#{value}")
      @tags_cache[name] = value
    end

    def delete_tag(name)
      run_command("tags", "-delete", name)
      @tags_cache.delete(name)
    end

    def update_cluster_id
      set_tag(Tag.cluster_id, cluster_id)
    end

    def set_have_unprocessed_messages_for(node_name)
      tag = Tag.have_unprocessed_messages_tag_for(node_name)
      set_tag(tag, true) unless @tags_cache.key?(tag)
    end

    def reset_have_unprocessed_messages_for(node_name)
      delete_tag(Tag.have_unprocessed_messages_tag_for(node_name))
    end

    def role
      NodeRole.normalize(get_tag(Tag.node_role))
    end

    def role=(new_role)
      role = NodeRole.normalize(new_role)
      set_tag(Tag.node_role, role)
      # after that you must run update_cluster_state to update the cluster information cache
      role
    end

    def last_processed_message_timestamp
      get_tag(Tag.last_processed_message_timestamp)
    end

    def last_processed_message_timestamp=(timestamp)
      set_tag(Tag.last_processed_message_timestamp, timestamp)
      # after that you must run update_cluster_state to update the cluster information cache
    end

    def accept_messages_newer_than_timestamp
      get_tag(Tag.accept_messages_newer_than)
    end

    def accept_messages_newer_than(timestamp)
      set_tag(Tag.accept_messages_newer_than, timestamp)
      # after that you must run update_cluster_state to update the cluster information cache
    end

    def cluster_id
      loader = Catalog::Loader.new(Path.catalog.to_s)
      catalog = loader.load
      catalog.cluster_id
    end

    CHECK_RESTARTED_INTERVAL = 3
    CHECK_RESTARTED_TIMEOUT = 60 * 5

    def ensure_restarted(*nodes, &block)
      nodes << @name.to_s if nodes.empty?

      targets = nodes.collect do |node|
        serf = self.class.new(node)
        {
          :serf          => serf,
          :previous_name => serf.get_tag(Tag.internal_node_name),
        }
      end

      start_time = Time.now

      yield # the given operation must restart the service.

      while Time.now - start_time < CHECK_RESTARTED_TIMEOUT
        puts "Checking nodes are restarted or not:" if @verbose
        targets.reject! do |target|
          name = target[:serf].get_tag(Tag.internal_node_name)
          restarted = name != target[:previous_name]
          puts "  #{name} vs #{target[:previous_name]} => " +
                 "#{restarted ? "restarted" : "not yet"}" if @verbose
          restarted
        end
        break if targets.empty?
        sleep(CHECK_RESTARTED_INTERVAL)
      end

      targets.empty?
    end

    private
    def ensure_serf
      @serf_command ||= find_system_serf
      return if @serf_command

      serf_path = Path.serf_command
      @serf_command = serf_path.to_s
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
      command = Command.new(@serf_command, command,
                            "-rpc-addr", rpc_address,
                            *options)
      command.verbose = @verbose
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
      node_name.to_s.split(":").first
    end

    def rpc_address
      "#{@name.host}:#{rpc_port}"
    end

    def rpc_port
      7373
    end

    def agent_port
      Agent::PORT
    end

    def detect_other_hosts
      loader = Catalog::Loader.new(Path.catalog.to_s)
      catalog = loader.load
      other_nodes = catalog.all_nodes.reject do |node|
        node == @name.to_s
      end
      other_nodes.collect do |node|
        NodeName.parse(node).host
      end
    end

    def log_tag
      "serf"
    end
  end
end
