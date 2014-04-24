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
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

require "droonga/plugin"
require "droonga/plugins/groonga/generic_command"

module Droonga
  module Plugins
    module Groonga
      module TableList
        HEADER = [
          ["id", "UInt32"],
          ["name","ShortText"],
          ["path","ShortText"],
          ["flags","ShortText"],
          ["domain", "ShortText"],
          ["range", "ShortText"],
          ["default_tokenizer","ShortText"],
          ["normalier","ShortText"],
        ].freeze

        class Command < GenericCommand
          def process_request(request)
            [HEADER, *list_tables]
          end

          private
          def list_tables
            @context.database.tables.collect do |table|
              format_table(table)
            end
          end

          def format_table(table)
            [
              table.id,
              table.name,
              table.path,
              table_flags(table),
              domain_name(table),
              range_name(table),
              default_tokenizer_name(table),
              normalizer_name(table),
            ]
          end

          def table_flags(table)
            flags = []
            case table
            when ::Groonga::Array
              flags << "TABLE_NO_KEY"
            when ::Groonga::Hash
              flags << "TABLE_HASH_KEY"
            when ::Groonga::PatriciaTrie
              flags << "TABLE_PAT_KEY"
            when ::Groonga::DoubleArrayTrie
              flags << "TABLE_DAT_KEY"
            end
            if table.domain
              if table.is_a?(::Groonga::PatriciaTrie) and
                   table.register_key_with_sis?
                flags << "KEY_WITH_SIS"
              end
            end
            flags << "PERSISTENT"
            flags.join("|")
          end

          def domain_name(table)
            return nil unless table.domain
            table.domain.name
          end

          def range_name(table)
            return nil unless table.range
            table.range.name
          end

          def default_tokenizer_name(table)
            return nil unless table.default_tokenizer
            table.default_tokenizer.name
          end

          def normalizer_name(table)
            return nil unless table.domain
            return nil unless table.normalizer
            table.normalizer.name
          end
        end

        class Handler < Droonga::Handler
          def handle(message)
            command = Command.new(@context)
            command.execute(message.request)
          end
        end

        Groonga.define_single_step do |step|
          step.name = "table_list"
          step.handler = Handler
          step.collector = Collectors::Or
        end
      end
    end
  end
end
