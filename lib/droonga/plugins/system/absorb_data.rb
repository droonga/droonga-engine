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
        action.synchronous = true

        DEFAULT_MESSAGES_PER_SECOND = 100

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
            dumper = Drndump::Dumper.new(dumper_params)

            serf = Serf.new(my_node_name)
            serf.set_tag("absorbing", true)

            begin
              @total_n_source_records = count_total_n_source_records
              @n_processed_messages = 0
              dumper_error_message = dumper.run do |message|
                @messenger.forward(message,
                                   "to"   => my_node_name,
                                   "type" => message["type"])
                @n_processed_messages += 1
                report_progress
              end
              report_progress
            rescue Exception => exception
              dumper_error_message = exception.to_s
            end

            serf.set_tag("absorbing", true)

            if dumper_error_message
              error(error_name, dumper_error_message)
            end
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

              :messages_per_second => params["messagesPerSecond"] || DEFAULT_MESSAGES_PER_SECOND,
            }
          end

          def report_progress
            return unless (@n_processed_messages % 100).zero?
            forward("#{prefix}.progress",
                    "nProcessedMessages" => @n_processed_messages,
                    "percentage"         => progress_percentage,
                    "message"            => progress_message)
          end

          def progress_percentage
            progress = @n_prosessed_messages / @total_n_source_records
            [(progress * 100).to_i, 100].min
          end

          ONE_MINUTE_IN_SECONDS = 60
          ONE_HOUR_IN_SECONDS = ONE_MINUTE_IN_SECONDS * 60

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

          def myself
            @myself ||= NodeName.parse(my_node_name)
          end

          def my_node_name
            @messenger.engine_state.name
          end

          def source_host
            @source_host ||= @request.request["host"]
          end

          def source_port
            @source_port ||= @request.request["port"] || NodeName::DEFAULT_PORT
          end

          def source_tag
            @source_tag ||= @request.request["tag"] || NodeName::DEFAULT_TAG
          end

          def source_dataset
            @source_dataset ||= @request.request["dataset"] || Catalog::Dataset::DEFAULT_NAME
          end

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

          def source_client_options
            params = @request.request
            options = {
              :host    => source_host,
              :port    => source_port,
              :tag     => source_tag,
              :dataset => source_dataset,

              :protocol => :droonga,

              :receiver_host => myself.host,
              :receiver_port => 0,
            }
          end

          def source_client
            @source_client ||= Droonga::Client.new(source_client_options)
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

          def log_tag
            "[#{Process.ppid}] data-absorber"
          end
        end

        def handle(message)
          raise MissingHostParameter.new unless message.include?("host")
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
