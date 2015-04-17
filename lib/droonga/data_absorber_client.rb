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

require "droonga/loggable"
require "droonga/node_name"
require "droonga/catalog/dataset"
require "droonga/client"
require "droonga/catalog/generator"
require "droonga/catalog/fetcher"

module Droonga
  class DataAbsorberClient
    include Loggable

    class DestinationEqualsToSource < StandardError
      def initialize(params)
        super("The source and the destination are same", params)
      end
    end

    DEFAULT_MESSAGES_PER_SECOND = 100
    DEFAULT_PROGRESS_INTERVAL_SECONDS = 3

    DEFAULT_HOST    = NodeName::DEFAULT_HOST
    DEFAULT_PORT    = NodeName::DEFAULT_PORT
    DEFAULT_TAG     = NodeName::DEFAULT_TAG
    DEFAULT_DATASET = Catalog::Dataset::DEFAULT_NAME

    attr_reader :params
    attr_reader :host, :port, :tag, :dataset
    attr_reader :messages_per_second, :progress_interval_seconds
    attr_reader :source_host, :source_port, :source_tag, :source_dataset
    attr_reader :error_message

    def initialize(params)
      @params = params

      @messages_per_second = @params[:messages_per_second] ||
                               DEFAULT_MESSAGES_PER_SECOND
      @progress_interval_seconds = @params[:progress_interval_seconds] ||
                                     DEFAULT_PROGRESS_INTERVAL_SECONDS

      @host    = @params[:host]    || DEFAULT_HOST
      @port    = @params[:port]    || DEFAULT_PORT
      @tag     = @params[:tag]     || DEFAULT_TAG
      @dataset = @params[:dataset] || DEFAULT_DATASET

      @source_host    = @params[:source_host]    || @host    || DEFAULT_HOST
      @source_port    = @params[:source_port]    || @port    || DEFAULT_PORT
      @source_tag     = @params[:source_tag]     || @tag     || DEFAULT_TAG
      @source_dataset = @params[:source_dataset] || @dataset || DEFAULT_DATASET

      @receiver_host = @params[:receiver_host] || @host
      @receiver_port = @params[:receiver_port] || 0

      @client_options = @params[:client_options] || {}

      @error_message = nil

      validate_params
    end

    def run
      n_absorbers = 0

      absorb_message = {
        "type" => "system.absorb-data",
        "dataset" => @dataset,
        "body" => {
          "host"    => @source_host,
          "port"    => @source_port,
          "tag"     => @source_tag,
          "dataset" => @source_dataset,
          "messagesPerSecond" => @messages_per_second,
          "progressIntervalSeconds" => @progress_interval_seconds,
        },
      }
      client = create_destination_client
      client.subscribe(absorb_message) do |message|
        case message
        when Droonga::Client::Error
          client.close
          @error_message = message.to_s
        else
          case message["type"]
          when "system.absorb-data.result", "system.absorb-data.error"
            if message["statusCode"] != 200
              client.close
              error = message["body"]
              @error_message = "#{error['name']}: #{error['message']}"
            end
          when "system.absorb-data.progress"
            body = message["body"]
            yield(:n_processed_messages => body["nProcessedMessages"],
                  :percentage           => body["percentage"],
                  :message              => body["message"])
          when "system.absorb-data.start"
            n_absorbers += 1
          when "system.absorb-data.end"
            n_absorbers -= 1
            client.close if n_absorbers <= 0
          end
        end
      end
    end

    def source_node_suspendable?
      (source_replica_hosts - [@source_host]).size >= 1
    end

    private
    def validate_params
      source_node_name = NodeName.new(:host => @source_host,
                                      :port => @source_port,
                                      :tag  => @source_tag)
      destination_node_name = NodeName.new(:host => @host,
                                           :port => @port,
                                           :tag  => @tag)
      if source_node_name == destination_node_name and
           @source_dataset == @dataset
        raise DestinationEqualsToSource.new(:host    => @host,
                                            :port    => @port,
                                            :tag     => @tag,
                                            :dataset => @dataset)
      end
    end

    def destination_client_options
      {
        :host          => @host,
        :port          => @port,
        :tag           => @tag,
        :protocol      => :droonga,
        :receiver_host => @receiver_host,
        :receiver_port => @receiver_port,
      }.merge(@client_options)
    end

    def create_destination_client
      Droonga::Client.new(destination_client_options)
    end

    def source_replica_hosts
      @source_replica_hosts ||= get_source_replica_hosts
    end

    def get_source_replica_hosts
      generator = Catalog::Generator.new
      generator.load(source_catalog)
      dataset = generator.dataset_for_host(@source_host)
      return [] unless dataset
      dataset.replicas.hosts
    end

    def source_catalog
      @source_catalog ||= fetch_source_catalog
    end

    def fetch_source_catalog
      fetcher = Catalog::Fetcher.new(:host          => @source_host,
                                     :port          => @source_port,
                                     :tag           => @source_tag,
                                     :receiver_host => @receiver_host)
      fetcher.fetch(:dataset => @source_dataset)
    end

    def log_tag
      "data-absorber"
    end
  end
end
