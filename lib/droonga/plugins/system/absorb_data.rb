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
require "droonga/catalog/dataset"
require "droonga/node_name"

require "drndump/dumper"

module Droonga
  module Plugins
    module System
      class AbsorbDataHandler < Droonga::Handler
        action.synchronous = true

        DEFAULT_MESSAGES_PER_SECOND = 100

        class MissingHostParameter < BadRequest
          def initialize
            super("\"host\" must be specified.")
          end
        end

        class DumpFailed < InternalServerError
          def initialize(error)
            super("source node returns an error.",
                  error)
          end
        end

        def handle(message)
          raise MissingHostParameter.new unless message.include?("host")

          dumper = Drndump::Dumper.new(dumper_params(message))

          serf = Serf.new(my_node_name)
          serf.set_tag("absorbing", true)

          error_message = dumper.run do |message|
            @messenger.forward(message,
                               "to"   => my_node_name,
                               "type" => message["type"])
          end

          serf.set_tag("absorbing", true)

          raise DumpFailed.new(error_message) if error_message

          true
        end

        private
        def dumper_params(message)
          {
            :host    => message["host"],
            :port    => message["port"]    || NodeName::DEFAULT_PORT,
            :tag     => message["tag"]     || NodeName::DEFAULT_TAG,
            :dataset => message["dataset"] || Catalog::Dataset::DEFAULT_NAME,

            :receiver_host => myself.host,
            :receiver_port => 0,

            :messages_per_second => message["messagesPerSecond"] || DEFAULT_MESSAGES_PER_SECOND,
          }
        end

        def myself
          @myself ||= NodeName.parse(my_node_name)
        end

        def my_node_name
          @messenger.engine_state.name
        end
      end

      define_single_step do |step|
        step.name = "system.absorb-data"
        step.handler = AbsorbDataHandler
        step.collector = Collectors::Or
      end
    end
  end
end
