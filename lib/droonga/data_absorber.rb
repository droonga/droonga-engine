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
    def initialize(params)
      @params = params

      @params[:messages_per_second] ||= DEFAULT_MESSAGES_PER_SECOND
      @params[:drndump] ||= "drndump"
      # We should use droonga-send instead of droonga-request,
      # because droonga-request is too slow.
      @params[:client] ||= "droonga-send"
    end

    def absorb
      drndump = @params[:drndump]
      drndump_options = []
      drndump_options += ["--host", @params[:source_host]] if @params[:source_host]
      drndump_options += ["--port", @params[:port].to_s] if @params[:port]
      drndump_options += ["--tag", @params[:tag]] if @params[:tag]
      drndump_options += ["--dataset", @params[:dataset]] if @params[:dataset]
      drndump_options += ["--receiver-host", @params[:destination_host]]
      drndump_options += ["--receiver-port", @params[:receiver_port].to_s] if @params[:receiver_port]

      client = @params[:client]
      client_options = []
      if client.include?("droonga-request")
        client_options += ["--host", @params[:destination_host]]
        client_options += ["--port", @params[:port].to_s] if @params[:port]
        client_options += ["--tag", @params[:tag]] if @params[:tag]
        client_options += ["--receiver-host", @params[:destination_host]]
        client_options += ["--receiver-port", @params[:receiver_port].to_s] if @params[:receiver_port]
      elsif client.include?("droonga-send")
        #XXX Don't use round-robin with multiple endpoints
        #    even if there are too much data.
        #    Schema and indexes must be sent to just one endpoint
        #    to keep their order, but currently there is no way to
        #    extract only schema and indexes via drndump.
        #    So, we always use just one endpoint for now,
        #    even if there are too much data.
        server = "droonga:#{params[:destination_host]}"
        server = "#{server}:#{params[:port].to_s}" if @params[:port]
        server = "#{server}/#{params[:tag].to_s}" if @params[:tag]
        client_options += ["--server", server]
        #XXX We should restrict the traffic to avoid overflowing!
        client_options += ["--messages-per-second", @params[:messages_per_second]]
      else
        raise ArgumentError.new("Unknwon type client: #{client}")
      end

      drndump_command_line = [drndump] + drndump_options
      client_command_line = [client] + client_options

      calculated_required_time = required_time_in_seconds
      unless calculated_required_time == TIME_UNKNOWN
        logger.info("calculated required time: #{calculated_required_time}sec")
        if block_given?
          yield(:required_time_in_seconds => calculated_required_time)
        end
      end

      start = Time.new.to_i
      env = {}
      Open3.pipeline_r([env, *drndump_command_line],
                       [env, *client_command_line]) do |last_stdout, thread|
        last_stdout.each do |output|
          progress = nil
          if calculated_required_time == TIME_UNKNOWN or
             calculated_required_time <= 0
            progress = PROGRESS_UNKNOWN
          else
            progress = (Time.new.to_i - start) / calculated_required_time
          end
          yield(:progress => progress,
                :output   => output) 
        end
      end
    end

    def required_time_in_seconds
      @params[:client].include?("droonga-send")
        total_n_source_records / @params[:messages_per_second]
      else
        TIME_UNKNOWN
      end
    end

    def source_client
      options = {
        :host          => @params[:source_host],
        :port          => @params[:port],
        :tag           => @params[:tag],
        :progocol      => :droonga,
        :receiver_host => @params[:destination_host],
        :receiver_port => 0,
      }
      @source_client ||= Droonga::Client.new(options)
    end

    def source_tables
      response = source_client.request("dataset" => @params[:dataset],
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
      response = source_client.request("dataset" => @params[:dataset],
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

    def log_tag
      "data-absorber"
    end
  end
end
