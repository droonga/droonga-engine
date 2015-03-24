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
require "droonga/node_metadata"
require "droonga/catalog_generator"
require "droonga/catalog_modifier"
require "droonga/catalog_fetcher"
require "droonga/data_absorber"
require "droonga/safe_file_writer"
require "droonga/service_installation"
require "droonga/restarter"

module Droonga
  module Command
    module Remote
      class Base
        attr_reader :response

        def initialize(serf_name, params)
          @serf_name = serf_name
          @params    = params
          @response  = {
            "log" => []
          }
          @serf = Serf.new(@serf_name)

          @service_installation = ServiceInstallation.new
          @service_installation.ensure_using_service_base_directory

          log("params = #{params}")
        end

        def process
          # override me!
        end

        def should_process?
          unless for_this_cluster?
            log("query for different cluster (to be ignroed)")
            return false
          end
          if for_me?
            log("query for this node (to be processed)")
            return true
          end
          if @params.nil? or not @params.include?("node")
            log("anonymous query (to be processed)")
            return true
          end
          log("query for different query (to be ignored)")
          return false
        end

        private
        def node
          @serf_name
        end

        def host
          node.split(":").first
        end

        def cluster_id
          @serf.cluster_id
        end

        def target_cluster
          @params && @params["cluster_id"]
        end

        def target_node
          @params && @params["node"]
        end

        def for_this_cluster?
          target_cluster.nil? or target_cluster == cluster_id
        end

        def for_me?
          target_node == @serf_name
        end

        def log(message)
          @response["log"] << message
        end
      end

      class ChangeRole < Base
        def process
          log("old role: #{@serf.role}")
          @serf.role = @params["role"]
          log("new role: #{@serf.role}")
        end
      end

      class ReportMetadata < Base
        def process
          metadata = NodeMetadata.new
          @response["value"] = metadata.get(@params["key"])
        end
      end

      class SetMetadata < Base
        def process
          metadata = NodeMetadata.new
          log("old value: #{metadata.get(@params["key"])}")
          metadata.set(@params["key"], @params["value"])
          log("new value: #{metadata.get(@params["key"])}")
          Restarter.restart
        end
      end

      class Join < Base
        def process
          log("type = #{type}")
          case type
          when "replica"
            join_as_replica
          end
        end

        private
        def type
          @params["type"]
        end

        def source_node
          @params["source"]
        end

        def joining_node
          @params["node"]
        end

        def dataset_name
          @params["dataset"]
        end

        def messages_per_second
          @params["messages_per_second"]
        end

        def valid_params?
          have_required_params? and
            valid_node?(source_node) and
            valid_node?(joining_node)
        end

        def have_required_params?
          required_params = [
            source_node,
            joining_node,
            dataset_name,
          ]
          required_params.all? do |param|
            not param.nil?
          end
        end

        NODE_PATTERN = /\A([^:]+):(\d+)\/(.+)\z/

        def valid_node?(node)
          node =~ NODE_PATTERN
        end

        def source_host
          @source_host ||= (source_node =~ NODE_PATTERN && $1)
        end

        def joining_host
          @joining_host ||= (joining_node =~ NODE_PATTERN && $1)
        end

        def port
          @port ||= (source_node =~ NODE_PATTERN && $2 && $2.to_i)
        end

        def tag
          @tag ||= (source_node =~ NODE_PATTERN && $3)
        end

        def join_as_replica
          return unless valid_params?

          log("source_node = #{source_node}")

          @catalog = fetch_catalog

          @other_hosts = replica_hosts
          log("other_hosts = #{@other_hosts}")
          return if @other_hosts.empty?

          join_to_cluster
        end

        def replica_hosts
          generator = CatalogGenerator.new
          generator.load(@catalog)
          dataset = generator.dataset_for_host(source_host) ||
                      generator.dataset_for_host(host)
          return [] unless dataset
          dataset.replicas.hosts
        end

        def fetch_catalog
          fetcher = CatalogFetcher.new(:host          => source_host,
                                       :port          => port,
                                       :tag           => tag,
                                       :receiver_host => host)
          fetcher.fetch(:dataset => dataset_name)
        end

        def join_to_cluster
          log("joining to the cluster")
          @serf.join(*@other_hosts)

          log("update catalog.json from fetched catalog")
          CatalogModifier.modify(@catalog) do |modifier, file|
            modifier.datasets[dataset_name].replicas.hosts += [joining_host]
            modifier.datasets[dataset_name].replicas.hosts.uniq!
            @service_installation.ensure_correct_file_permission(file)
          end
          log("done")
        end
      end

      class AbsorbData < Base
        attr_writer :dataset_name, :port, :tag

        def process
          return unless source

          log("start to absorb data from #{source}")

          if dataset_name.nil? or port.nil? or tag.nil?
            current_catalog = JSON.parse(Path.catalog.read)
            generator = CatalogGenerator.new
            generator.load(current_catalog)

            dataset = generator.dataset_for_host(source)
            return unless dataset

            self.dataset_name = dataset.name
            self.port         = dataset.replicas.port
            self.tag          = dataset.replicas.tag
          end

          log("dataset = #{dataset_name}")
          log("port    = #{port}")
          log("tag     = #{tag}")

          metadata = NodeMetadata.new
          metadata.set(:absorbing, true)
          Restarter.restart(5)

          DataAbsorber.absorb(:dataset          => dataset_name,
                              :source_host      => source,
                              :destination_host => host,
                              :port             => port,
                              :tag              => tag,
                              :messages_per_second => messages_per_second,
                              :client           => "droonga-send")

          metadata.delete(:absorbing)
          Restarter.restart(5)
        end

        private
        def source
          @params["source"]
        end

        def dataset_name
          @dataset_name ||= @params["dataset"]
        end

        def port
          @port ||= @params["port"]
        end

        def tag
          @tag ||= @params["tag"]
        end

        def messages_per_second
          @messages_per_second ||= @params["messages_per_second"]
        end
      end

      class ModifyReplicasBase < Base
        private
        def dataset
          @params["dataset"]
        end

        def hosts
          @hosts ||= prepare_hosts
        end

        def prepare_hosts
          hosts = @params["hosts"]
          return nil unless hosts
          hosts = [hosts] if hosts.is_a?(String)
          hosts
        end
      end

      class SetReplicas < ModifyReplicasBase
        def process
          return if dataset.nil? or hosts.nil?

          log("new replicas: #{hosts.join(",")}")

          log("joining to the cluster")
          @serf.join(*hosts)

          log("setting replicas to the cluster")
          CatalogModifier.modify do |modifier, file|
            modifier.datasets[dataset].replicas.hosts = hosts
            @service_installation.ensure_correct_file_permission(file)
          end
          log("done")
        end
      end

      class AddReplicas < ModifyReplicasBase
        def process
          return if dataset.nil? or hosts.nil?

          added_hosts = hosts - [host]
          log("adding replicas: #{added_hosts.join(",")}")
          return if added_hosts.empty?

          log("joining to the cluster")
          @serf.join(*added_hosts)

          log("adding replicas to the cluster")
          CatalogModifier.modify do |modifier, file|
            modifier.datasets[dataset].replicas.hosts += added_hosts
            modifier.datasets[dataset].replicas.hosts.uniq!
            @service_installation.ensure_correct_file_permission(file)
          end
          log("done")
        end
      end

      class RemoveReplicas < ModifyReplicasBase
        def process
          return if dataset.nil? or hosts.nil?

          log("removing replicas: #{hosts.join(",")}")

          log("removing replicas from the cluster")
          CatalogModifier.modify do |modifier, file|
            modifier.datasets[dataset].replicas.hosts -= hosts
            @service_installation.ensure_correct_file_permission(file)
          end
          log("done")
        end
      end

      class Unjoin < ModifyReplicasBase
        def process
          return if dataset.nil? or hosts.nil?

          log("unjoining replicas: #{hosts.join(",")}")

          log("unjoining from the cluster")
          CatalogModifier.modify do |modifier, file|
            if unjoining_node?
              modifier.datasets[dataset].replicas.hosts = hosts
            else
              modifier.datasets[dataset].replicas.hosts -= hosts
            end
            @service_installation.ensure_correct_file_permission(file)
          end
          log("done")
        end

        private
        def unjoining_node?
          hosts.include?(host)
        end
      end

      class UpdateClusterState < Base
        def process
          log("updating cluster state")
          @serf.update_cluster_state
          log("done")
        end
      end
    end
  end
end
