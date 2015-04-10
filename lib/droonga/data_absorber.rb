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
require "droonga/client"
require "droonga/catalog_generator"
require "droonga/catalog_fetcher"

module Droonga
  class DataAbsorber
    include Loggable

    DEFAULT_MESSAGES_PER_SECOND = 100
    DEFAULT_PROGRESS_INTERVAL_SECONDS = 3

    TIME_UNKNOWN = -1
    PROGRESS_UNKNOWN = -1

    class << self
      def absorb(params)
        new(params).absorb
      end
    end

    attr_reader :params
    attr_reader :dataset, :port, :tag, :messages_per_second
    attr_reader :source_host, :destination_host
    attr_reader :error_message

    def initialize(params)
      @params = params

      @messages_per_second = @params[:messages_per_second] || DEFAULT_MESSAGES_PER_SECOND
      @progress_interval_seconds = @params[:progress_interval_seconds] || DEFAULT_PROGRESS_INTERVAL_SECONDS

      @dataset = @params[:dataset] || CatalogGenerator::DEFAULT_DATASET
      @port    = @params[:port]    || CatalogGenerator::DEFAULT_PORT
      @tag     = @params[:tag]     || CatalogGenerator::DEFAULT_TAG

      @source_host      = @params[:source_host]
      @destination_host = @params[:destination_host]
      @receiver_host    = @params[:receiver_host] || @destination_host

      @receiver_port = @params[:receiver_port]

      @client_options = @params[:client_options] || {}

      @error_message = nil
    end

    def run
      n_absorbers = 0

      absorb_message = {
        "type" => "system.absorb-data",
        "body" => {
          "host"    => @source_host,
          "port"    => @port,
          "tag"     => @tag,
          "dataset" => @dataset,
          "messagesPerSecond" => @messages_per_second,
          "progressIntervalSeconds" => @progress_interval_seconds,
        },
      }
      destination_client.subscribe(absorb_message) do |message|
        case message
        when Droonga::Client::Error
          destination_client.close
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

    def destination_client
      options = {
        :host          => @destination_host,
        :port          => @port,
        :tag           => @tag,
        :protocol      => :droonga,
        :receiver_host => @receiver_host,
        :receiver_port => 0,
      }.merge(@client_options)
      @destination_client ||= Droonga::Client.new(options)
    end

    def source_node_suspendable?
      (source_replica_hosts - [@source_host]).size >= 1
    end

    private
    def source_replica_hosts
      @source_replica_hosts ||= get_source_replica_hosts
    end

    def get_source_replica_hosts
      generator = CatalogGenerator.new
      generator.load(source_catalog)
      dataset = generator.dataset_for_host(@source_host)
      return [] unless dataset
      dataset.replicas.hosts
    end

    def source_catalog
      @source_catalog ||= fetch_source_catalog
    end

    def fetch_source_catalog
      fetcher = CatalogFetcher.new(:host          => @source_host,
                                   :port          => @port,
                                   :tag           => @tag,
                                   :receiver_host => @receiver_host)
      fetcher.fetch(:dataset => @dataset)
    end

    def log_tag
      "data-absorber"
    end
  end
end
