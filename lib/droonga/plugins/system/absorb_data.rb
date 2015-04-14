# Copyright (C) 2015 Droonga Project
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

require "droonga/plugin"
require "droonga/plugin/async_command"
require "droonga/catalog/dataset"
require "droonga/serf"
require "droonga/node_name"

require "drndump/dumper"

module Droonga
  module Plugins
    module System
      class AbsorbDataHandler < AsyncCommand::Handler
        DEFAULT_MESSAGES_PER_SECOND = 100
        DEFAULT_PROGRESS_INTERVAL_SECONDS = 3
        MIN_PROGRESS_INTERVAL_SECONDS = 1

        class MissingHostParameter < BadRequest
          def initialize
            super("\"host\" must be specified.")
          end
        end

        class DataAbsorber < AsyncCommand::AsyncHandler
          class EmptyResponse < StandardError
          end

          class EmptyBody < StandardError
          end

          private
          def prefix
            "system.absorb-data"
          end

          def error_name
            "AbsorbFailure"
          end

          def error_message
            "failed to absorb data"
          end

          def handle
            @dumper_error_message = nil

            dumper = Drndump::Dumper.new(dumper_params)
            dumper.on_finish = lambda do
              on_finish
            end

            @start_time = Time.now

            begin
              @n_processed_messages = 0
              @total_n_source_records = nil
              get_total_n_source_records do |count|
                @total_n_source_records = count
              end
              @dumper_error_message = dumper.run do |message|
                begin
                  message["dataset"] = current_dataset
                  @messenger.forward(message,
                                     "to"   => my_node_name,
                                     "type" => message["type"])
                  @n_processed_messages += 1
                  elapsed_seconds = (Time.now - @start_time).to_i
                  if (elapsed_seconds % progress_interval_seconds).zero?
                    report_progress
                  end
                rescue Exception => exception
                  @dumper_error_message
                  on_finish
                end
              end
            rescue Exception => exception
              @dumper_error_message = exception.to_s
            end

            on_finish if @dumper_error_message
          end

          def on_finish
            begin
              if @dumper_error_message
                error(error_name, @dumper_error_message)
              else
                @total_n_source_records = @n_processed_messages
                report_progress
              end
            rescue Exception => exception
              @dumper_error_message = exception.to_s
              error(error_name, @dumper_error_message)
            end
            forward("#{prefix}.end")
          end

          def dumper_params
            params = @request.request
            {
              :host    => source_host,
              :port    => source_port,
              :tag     => source_tag,
              :dataset => source_dataset,

              :receiver_host => myself.host,
              :receiver_port => 0,

              :client_options => {
                :backend => :coolio,
                :loop    => @loop,
              },

              :messages_per_second => messages_per_second,
            }
          end

          def report_progress
            if @n_processed_messages.nil? or @total_n_source_records.nil?
              return
            end
            forward("#{prefix}.progress",
                    "nProcessedMessages" => @n_processed_messages,
                    "percentage"         => progress_percentage,
                    "message"            => progress_message)
          end

          def progress_percentage
            if @total_n_source_records.nil? or @total_n_source_records.zero?
              return 0
            end
            progress = @n_processed_messages / @total_n_source_records
            [(progress * 100).to_i, 100].min
          end

          ONE_MINUTE_IN_SECONDS = 60
          ONE_HOUR_IN_SECONDS = ONE_MINUTE_IN_SECONDS * 60

          def progress_message
            n_remaining_records = [@total_n_source_records - @n_processed_messages, 0].max

            remaining_seconds  = n_remaining_records / messages_per_second
            remaining_hours    = (remaining_seconds / ONE_HOUR_IN_SECONDS).floor
            remaining_seconds -= remaining_hours * ONE_HOUR_IN_SECONDS
            remaining_minutes  = (remaining_seconds / ONE_MINUTE_IN_SECONDS).floor
            remaining_seconds -= remaining_minutes * ONE_MINUTE_IN_SECONDS
            remaining_time     = sprintf("%02i:%02i:%02i",
                                         remaining_hours,
                                         remaining_minutes,
                                         remaining_seconds)

            "#{progress_percentage}% done (maybe #{remaining_time} remaining)"
          end

          def myself
            @myself ||= NodeName.parse(my_node_name)
          end

          def my_node_name
            ENV["DROONGA_ENGINE_NAME"]
          end

          def current_dataset
            @request.dataset
          end

          def prepare_progress_interval_seconds
            interval_seconds = @request.request["progressIntervalSeconds"] ||
                                 DEFAULT_PROGRESS_INTERVAL_SECONDS
            interval_seconds = interval_seconds.to_i
            [interval_seconds, MIN_PROGRESS_INTERVAL_SECONDS].max
          end

          def progress_interval_seconds
            @progress_interval_seconds ||= prepare_progress_interval_seconds
          end

          def source_host
            @source_host ||= @request.request["host"]
          end

          def source_port
            @source_port ||= @request.request["port"] ||
                               NodeName::DEFAULT_PORT
          end

          def source_tag
            @source_tag ||= @request.request["tag"] ||
                              NodeName::DEFAULT_TAG
          end

          def source_dataset
            @source_dataset ||= @request.request["dataset"] ||
                                  Catalog::Dataset::DEFAULT_NAME
          end

          def messages_per_second
            @messages_per_second ||= @request.request["messagesPerSecond"] ||
                                       DEFAULT_MESSAGES_PER_SECOND
          end

          def source_client_options
            {
              :host    => source_host,
              :port    => source_port,
              :tag     => source_tag,
              :dataset => source_dataset,

              :protocol => :droonga,

              :receiver_host => myself.host,
              :receiver_port => 0,

              :backend => :coolio,
              :loop    => @loop,
            }
          end

          def source_client
            @source_client ||= Droonga::Client.new(source_client_options)
          end

          def get_source_tables(&block)
            source_client.request("dataset" => source_dataset,
                                  "type"    => "table_list") do |response|
              unless response
                raise EmptyResponse.new("table_list returns nil response")
              end
              unless response["body"]
                raise EmptyBody.new("table_list returns nil result")
              end

              message_body = response["body"]
              body = message_body[1]
              tables = body[1..-1]
              table_names = tables.collect do |table|
                table[1]
              end
              yield(table_names)
            end
          end

          def get_total_n_source_records(&block)
            get_source_tables do |source_tables|
              queries = {}
              source_tables.each do |table|
                queries["n_records_of_#{table}"] = {
                  "source" => table,
                  "output" => {
                    "elements" => ["count"],
                  },
                }
              end
              source_client.request("dataset" => source_dataset,
                                    "type"    => "search",
                                    "body"    => {
                                      "timeout" => 10,
                                      "queries" => queries,
                                    }) do |response|
                unless response
                  raise EmptyResponse.new("search returns nil response")
                end
                unless response["body"]
                  raise EmptyBody.new("search returns nil result")
                end

                n_records = 0
                response["body"].each do |query_name, result|
                  n_records += result["count"]
                end
                yield(n_records)
              end
            end
          end

          def log_tag
            "[#{Process.ppid}] data-absorber"
          end
        end

        def handle(message)
          unless message.request.include?("host")
            raise MissingHostParameter.new
          end
          super
        end

        private
        def start(request)
          absorber = DataAbsorber.new(loop, messenger, request)
          absorber.start
        end
      end

      define_single_step do |step|
        step.name = "system.absorb-data"
        step.handler = AbsorbDataHandler
        step.collector = Collectors::And
      end
    end
  end
end
