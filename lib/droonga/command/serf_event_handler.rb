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
require "droonga/node_status"
require "droonga/catalog_generator"
require "droonga/catalog_modifier"
require "droonga/catalog_fetcher"
require "droonga/data_absorber"
require "droonga/safe_file_writer"

module Droonga
  module Command
    module Remote
      class Base
        attr_reader :response

        def initialize
          @serf_name = ENV["SERF_SELF_NAME"]
          @response = {
            "log" => []
          }
          @payload = JSON.parse($stdin.gets)
        end

        def process
          # override me!
        end

        def should_process?
          for_me? or @payload.nil? or not @payload.include?("node")
        end

        private
        def node
          @serf_name
        end

        def host
          node.split(":").first
        end

        def target_node
          @payload && @payload["node"]
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
          NodeStatus.set(:role, @payload["role"])
        end
      end

      class ReportStatus < Base
        def process
          @response["value"] = NodeStatus.get(@payload["key"])
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
          @payload["type"]
        end

        def source_node
          @payload["source"]
        end

        def joining_node
          @payload["node"]
        end

        def dataset_name
          @payload["dataset"]
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
          @source_host ||= (joining_node =~ NODE_PATTERN && $1)
        end

        def port
          @port ||= (source_node =~ NODE_PATTERN && $2 && $2.to_i)
        end

        def tag
          @tag ||= (source_node =~ NODE_PATTERN && $3)
        end

        def should_absorb_data?
          @payload["copy"]
        end

        def join_as_replica
          return unless valid_params?

          log("source_node  = #{source_node}")

          fetcher = CatalogFetcher.new(:host          => source_host,
                                       :port          => port,
                                       :tag           => tag,
                                       :receiver_host => joining_host)
          catalog = fetcher.fetch(:dataset => dataset_name)

          generator = CatalogGenerator.new
          generator.load(catalog)
          dataset = generator.dataset_for_host(source_host) ||
                      generator.dataset_for_host(host)
          return unless dataset

          # restart self with the fetched catalog.
          SafeFileWriter.write(Path.catalog, JSON.pretty_generate(catalog))

          other_hosts  = dataset.replicas.hosts

          absorb_data if should_absorb_data?

          log("joining to the cluster: update myself")

          CatalogModifier.modify do |modifier|
            modifier.datasets[dataset_name].replicas.hosts += other_hosts
            modifier.datasets[dataset_name].replicas.hosts.uniq!
          end
        end

        def absorb_data
          log("starting to copy data from #{source_host}")

          CatalogModifier.modify do |modifier|
            modifier.datasets[dataset_name].replicas.hosts = [host]
          end
          sleep(5) #TODO: wait for restart. this should be done more safely, to avoid starting of absorbing with old catalog.json.

          status = NodeStatus.new
          status.set(:absorbing, true)
          DataAbsorber.absorb(:dataset          => dataset_name,
                              :source_host      => source_host,
                              :destination_host => joining_host,
                              :port             => port,
                              :tag              => tag)
          status.delete(:absorbing)
          sleep(1)
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

          status = NodeStatus.new
          status.set(:absorbing, true)
          DataAbsorber.absorb(:dataset          => dataset_name,
                              :source_host      => source,
                              :destination_host => host,
                              :port             => port,
                              :tag              => tag,
                              :client           => "droonga-send")
          status.delete(:absorbing)
        end

        private
        def source
          @payload["source"]
        end

        def dataset_name
          @dataset_name ||= @payload["dataset"]
        end

        def port
          @port ||= @payload["port"]
        end

        def tag
          @tag ||= @payload["tag"]
        end
      end

      class ModifyReplicasBase < Base
        private
        def dataset
          @payload["dataset"]
        end

        def hosts
          @hosts ||= prepare_hosts
        end

        def prepare_hosts
          hosts = @payload["hosts"]
          return nil unless hosts
          hosts = [hosts] if hosts.is_a?(String)
          hosts
        end
      end

      class SetReplicas < ModifyReplicasBase
        def process
          return unless dataset
          return unless hosts

          log("new replicas: #{hosts.join(",")}")

          CatalogModifier.modify do |modifier|
            modifier.datasets[dataset].replicas.hosts = hosts
          end
        end
      end

      class AddReplicas < ModifyReplicasBase
        def process
          return unless dataset
          return unless hosts

          hosts -= [host]
          return if hosts.empty?

          log("adding replicas: #{hosts.join(",")}")

          CatalogModifier.modify do |modifier|
            modifier.datasets[dataset].replicas.hosts += hosts
            modifier.datasets[dataset].replicas.hosts.uniq!
          end
        end
      end

      class RemoveReplicas < ModifyReplicasBase
        def process
          return unless dataset
          return unless hosts

          log("removing replicas: #{hosts.join(",")}")

          CatalogModifier.modify do |modifier|
            modifier.datasets[dataset].replicas.hosts -= hosts
          end
        end
      end

      class UpdateLiveNodes < Base
        def process
          def live_nodes
            Serf.live_nodes(@serf_name)
          end

          def output_live_nodes
            path = Path.live_nodes
            nodes = live_nodes
            file_contents = JSON.pretty_generate(nodes)
            SafeFileWriter.write(path, file_contents)
          end
        end
      end
    end

    class SerfEventHandler
      class << self
        def run
          new.run
        end
      end

      def run
        command_class = detect_command_class
        return true if command_class.nil?

        command = command_class.new
        command.process if command.should_process?
        output_response(command.response)
        true
      end

      private
      def detect_command_class
        case ENV["SERF_EVENT"]
        when "user"
          detect_command_class_from_custom_event(ENV["SERF_USER_EVENT"])
        when "query"
          detect_command_class_from_custom_event(ENV["SERF_QUERY_NAME"])
        when "member-join", "member-leave", "member-update", "member-reap"
          Remote::UpdateLiveNodes
        end
      end

      def detect_command_class_from_custom_event(event_name)
        case event_name
        when "change_role"
          Remote::ChangeRole
        when "report_status"
          Remote::ReportStatus
        when "join"
          Remote::Join
        when "set_replicas"
          Remote::SetReplicas
        when "add_replicas"
          Remote::AddReplicas
        when "remove_replicas"
          Remote::RemoveReplicas
        when "absorb_data"
          Remote::AbsorbData
        else
          nil
        end
      end

      def output_response(response)
        puts JSON.generate(response)
      end
    end
  end
end
