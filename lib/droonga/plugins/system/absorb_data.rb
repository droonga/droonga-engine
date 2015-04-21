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

require "drndump/dump_client"

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

          def start
            logger.trace("start: start")
            on_start

            @dumper_error_message = nil

            @dumper = Drndump::DumpClient.new(dumper_params)
            @dumper.on_finish = lambda do
              on_finish
              logger.trace("start: finish")
            end
            @dumper.on_progress = lambda do |message|
              logger.trace("dump progress",
                           :message => message)
            end
            @dumper.on_error = lambda do |error|
              logger.error("unexpected error while dump",
                           :error => error)
            end

            @previous_report_time = Time.now

            begin
              logger.info("starting to absorb the source dataset")
              @dumper_error_message = @dumper.run(dump_options) do |message|
                begin
                  message["dataset"] = current_dataset
                  message["xSender"] = "system.absorb-data"
                  @messenger.forward(message,
                                     "to"   => my_node_name,
                                     "type" => message["type"])
                  now = Time.now
                  elapsed_seconds = (now - @previous_report_time).to_i
                  if elapsed_seconds >= progress_interval_seconds
                    @previous_report_time = now
                    report_progress
                  end
                rescue Exception => exception
                  @dumper_error_message = exception.to_s
                  logger.exception("failed to process progress",
                                   exception)
                  on_finish
                end
              end
            rescue Exception => exception
              @dumper_error_message = exception.to_s
              logger.exception("failed to start dump",
                               exception)
            end

            on_finish if @dumper_error_message
            logger.trace("start: done")
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

          def on_finish
            begin
              if @dumper_error_message
                error(error_name, @dumper_error_message)
              else
                report_progress
              end
            rescue Exception => exception
              @dumper_error_message = exception.to_s
              logger.exception("failed to finish dump",
                               exception)
              error(error_name, @dumper_error_message)
            end
            super
          end

          def dumper_params
            {
              :host    => source_host,
              :port    => source_port,
              :tag     => source_tag,
              :dataset => source_dataset,

              :receiver_host => myself.host,
              :receiver_port => 0,
            }
          end

          def dump_options
            {
              :backend => :coolio,
              :loop    => @loop,
              :messages_per_second => messages_per_second,
            }
          end

          def report_progress
            message = "#{@dumper.progress_percentage}% done " +
                        "(maybe #{@dumper.formatted_remaining_time} remaining)"
            forward("#{prefix}.progress",
                    "nProcessedMessages" => @dumper.n_received_messages,
                    "percentage"         => @dumper.progress_percentage,
                    "message"            => message)
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
        step.single_operation = true
        step.handler = AbsorbDataHandler
        step.collector = Collectors::And
      end
    end
  end
end
