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

require "open3"

require "droonga/loggable"
require "droonga/client"
require "droonga/catalog_generator"
require "droonga/catalog_fetcher"

module Droonga
  class DataAbsorber
    include Loggable

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
    attr_reader :source_host, :destination_host, :receiver_host, :receiver_port

    def initialize(params)
      @params = params

      @messages_per_second = @params[:messages_per_second] || DEFAULT_MESSAGES_PER_SECOND

      @drndump = @params[:drndump] || "drndump"
      # We should use droonga-send instead of droonga-request,
      # because droonga-request is too slow.
      @client = @params[:client] || "droonga-send"

      @dataset = @params[:dataset] || CatalogGenerator::DEFAULT_DATASET
      @port    = @params[:port]    || CatalogGenerator::DEFAULT_PORT
      @tag     = @params[:tag]     || CatalogGenerator::DEFAULT_TAG

      @source_host      = @params[:source_host]
      @destination_host = @params[:destination_host]
      @receiver_host    = @params[:receiver_host] || @destination_host

      @receiver_port = @params[:receiver_port]
    end

    MESSAGES_PER_SECOND_MATCHER = /(\d+(\.\d+)?) messages\/second/

    def absorb
      drndump_command_line = [@drndump] + drndump_options
      client_command_line  = [@client] + client_options(@client)

      start_time_in_seconds = Time.new.to_i
      env = {}
      Open3.pipeline_r([env, *drndump_command_line],
                       [env, *client_command_line]) do |last_stdout, thread|
        last_stdout.each do |output|
          if block_given?
            messages_per_second = nil
            if output =~ MESSAGES_PER_SECOND_MATCHER
              messages_per_second = $1.to_f
            end
            yield(:progress => report_progress(start_time_in_seconds),
                  :output   => output,
                  :messages_per_second => messages_per_second)
          end
        end
      end
    end

    def can_report_remaining_time?
      required_time_in_seconds != Droonga::DataAbsorber::TIME_UNKNOWN and
        required_time_in_seconds > 0
    end

    def required_time_in_seconds
      @required_time_in_seconds ||= calculate_required_time_in_seconds
    end

    ONE_MINUTE_IN_SECONDS = 60
    ONE_HOUR_IN_SECONDS = ONE_MINUTE_IN_SECONDS * 60

    def report_progress(start_time_in_seconds)
      return nil unless can_report_remaining_time?

      elapsed_time = Time.new.to_i - start_time_in_seconds
      progress = elapsed_time.to_f / required_time_in_seconds
      progress = [(progress * 100).to_i, 100].min

      remaining_seconds  = [required_time_in_seconds - elapsed_time, 0].max
      remaining_hours    = (remaining_seconds / ONE_HOUR_IN_SECONDS).floor
      remaining_seconds -= remaining_hours * ONE_HOUR_IN_SECONDS
      remaining_minutes  = (remaining_seconds / ONE_MINUTE_IN_SECONDS).floor
      remaining_seconds -= remaining_minutes * ONE_MINUTE_IN_SECONDS
      remaining_time     = sprintf("%02i:%02i:%02i", remaining_hours, remaining_minutes, remaining_seconds)

      "#{progress}% done (maybe #{remaining_time} remaining)"
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
      }
      @destination_client ||= Droonga::Client.new(options)
    end

    def source_node_suspendable?
      (source_replica_hosts - [@source_host]).size >= 1
    end

    private
    def calculate_required_time_in_seconds
      if @client.include?("droonga-send")
        total_n_source_records / @messages_per_second
      else
        TIME_UNKNOWN
      end
    end

    def drndump_options
      options = []
      options += ["--host", @source_host] if @source_host
      options += ["--port", @port]
      options += ["--tag", @tag]
      options += ["--dataset", @dataset]
      options += ["--receiver-host", @receiver_host]
      options += ["--receiver-port", @receiver_port] if @receiver_port
      options.collect(&:to_s)
    end

    def droonga_request_options
      options = []
      options += ["--host", @destination_host]
      options += ["--port", @port]
      options += ["--tag", @tag]
      options += ["--receiver-host", @receiver_host]
      options += ["--receiver-port", @receiver_port] if @receiver_port
      options.collect(&:to_s)
    end

    def droonga_send_options
      options = []
  
      #XXX Don't use round-robin with multiple endpoints
      #    even if there are too much data.
      #    Schema and indexes must be sent to just one endpoint
      #    to keep their order, but currently there is no way to
      #    extract only schema and indexes via drndump.
      #    So, we always use just one endpoint for now,
      #    even if there are too much data.
      server = "droonga:#{params[:destination_host]}"
      server = "#{server}:#{params[:port].to_s}"
      server = "#{server}/#{params[:tag].to_s}"
      options += ["--server", server]
  
      #XXX We should restrict the traffic to avoid overflowing!
      options += ["--messages-per-second", @messages_per_second]

      options += ["--report-throughput"]
  
      options.collect(&:to_s)
    end

    def client_options(client)
      if client.include?("droonga-request")
        droonga_request_options
      elsif client.include?("droonga-send")
        droonga_send_options
      else
        raise ArgumentError.new("Unknwon type client: #{client}")
      end
    end

    def source_tables
      response = source_client.request("dataset" => @dataset,
                                       "type"    => "table_list")
      body = response["body"][1]
      tables = body[1..-1]
      tables.collect do |table|
        table[1]
      end
    end

    def total_n_source_records
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
