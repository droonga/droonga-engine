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

            dumper_error_message = dumper.run do |message|
              @messenger.forward(message,
                                 "to"   => my_node_name,
                                 "type" => message["type"])
              forward("#{prefix}.progress")
            end

            serf.set_tag("absorbing", true)

            if dumper_error_message
              error(error_name, dumper_error_message)
            end
          end

          def dumper_params
            params = @request.request
            {
              :host    => params["host"],
              :port    => params["port"]    || NodeName::DEFAULT_PORT,
              :tag     => params["tag"]     || NodeName::DEFAULT_TAG,
              :dataset => params["dataset"] || Catalog::Dataset::DEFAULT_NAME,

              :receiver_host => myself.host,
              :receiver_port => 0,

              :messages_per_second => params["messagesPerSecond"] || DEFAULT_MESSAGES_PER_SECOND,
            }
          end

          def myself
            @myself ||= NodeName.parse(my_node_name)
          end

          def my_node_name
            @messenger.engine_state.name
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
