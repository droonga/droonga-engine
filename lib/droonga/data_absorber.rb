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

    class EmptyResponse < StandardError
    end

    class EmptyBody < StandardError
    end

    DEFAULT_MESSAGES_PER_SECOND = 100

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

      @dataset = @params[:dataset] || CatalogGenerator::DEFAULT_DATASET
      @port    = @params[:port]    || CatalogGenerator::DEFAULT_PORT
      @tag     = @params[:tag]     || CatalogGenerator::DEFAULT_TAG

      @source_host      = @params[:source_host]
      @destination_host = @params[:destination_host]
      @receiver_host    = @params[:receiver_host] || @destination_host

      @receiver_port = @params[:receiver_port]

      @destination_client_options = @params[:client_options] || {}

      @error_message = nil

      #XXX We must instantiate the number of total soruce records before absorbing,
      #    because parallel commands while doing "dump" can be timed out.
      @total_n_source_records = count_total_n_source_records
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
            @n_prosessed_messages = message["body"]["count"]
            yield(:n_processed_messages => @n_processed_messages,
                  :percentage           => progress_percentage,
                  :message              => progress_message)
          when "system.absorb-data.start"
            n_absorbers += 1
          when "system.absorb-data.end"
            n_absorbers -= 1
            client.close if n_absorbers <= 0
          end
        end
      end
    end

    ONE_MINUTE_IN_SECONDS = 60
    ONE_HOUR_IN_SECONDS = ONE_MINUTE_IN_SECONDS * 60

    def progress_percentage
      progress = @n_prosessed_messages / @total_n_source_records
      [(progress * 100).to_i, 100].min
    end

    def progress_message
      n_remaining_records = [@total_n_source_records - @n_prosessed_messages, 0].max

      remaining_seconds  = n_remaining_records / @messages_per_second
      remaining_hours    = (remaining_seconds / ONE_HOUR_IN_SECONDS).floor
      remaining_seconds -= remaining_hours * ONE_HOUR_IN_SECONDS
      remaining_minutes  = (remaining_seconds / ONE_MINUTE_IN_SECONDS).floor
      remaining_seconds -= remaining_minutes * ONE_MINUTE_IN_SECONDS
      remaining_time     = sprintf("%02i:%02i:%02i", remaining_hours, remaining_minutes, remaining_seconds)

      "#{progress_percentage}% done (maybe #{remaining_time} remaining)"
    end

    def source_client
      options = {
        :host          => @source_host,
        :port          => @port,
        :tag           => @tag,
        :progocol      => :droonga,
        :receiver_host => @receiver_host,
        :receiver_port => 0,
      }
      @source_client ||= Droonga::Client.new(options)
    end

    def destination_client
      options = {
        :host          => @destination_host,
        :port          => @port,
        :tag           => @tag,
        :progocol      => :droonga,
        :receiver_host => @receiver_host,
        :receiver_port => 0,
      }.merge(@destination_client_options)
      @destination_client ||= Droonga::Client.new(options)
    end

    def source_node_suspendable?
      (source_replica_hosts - [@source_host]).size >= 1
    end

    private
    def source_tables
      response = source_client.request("dataset" => @dataset,
                                       "type"    => "table_list")

      raise EmptyResponse.new("table_list") unless response
      raise EmptyBody.new("table_list") unless response["body"]

      message_body = response["body"]
      body = message_body[1]
      tables = body[1..-1]
      tables.collect do |table|
        table[1]
      end
    end

    def count_total_n_source_records
      queries = {}
      source_tables.each do |table|
        queries["n_records_of_#{table}"] = {
          "source" => table,
          "output" => {
            "elements" => ["count"],
          },
        }
      end
      response = source_client.request("dataset" => @dataset,
                                       "type"    => "search",
                                       "body"    => {
                                         "queries" => queries,
                                       })

      raise EmptyResponse.new("search") unless response
      raise EmptyBody.new("search") unless response["body"]

      n_records = 0
      response["body"].each do |query_name, result|
        n_records += result["count"]
      end
      n_records
    end

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
