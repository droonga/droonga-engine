# Copyright (C) 2013-2014 Droonga Project
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

require "groonga/command/table-create"

require "droonga/plugin"
require "droonga/plugins/groonga/generic_command"

module Droonga
  module Plugins
    module Groonga
      module TableCreate
        class Command < GenericCommand
          def process_request(request)
            command_class = ::Groonga::Command.find("table_create")
            @command = command_class.new("table_create", request)

            name = @command["name"]
            unless name
              message = "Should not create anonymous table"
              raise CommandError.new(:status => Status::INVALID_ARGUMENT,
                                     :message => message,
                                     :result => false)
            end

            options = parse_command
            ::Groonga::Schema.define(:context => @context) do |schema|
              schema.create_table(name, options)
            end
            true
          end

          private
          def parse_command
            options = {}
            parse_flags(options)
            parse_key_type(options)
            parse_value_type(options)
            parse_default_tokenizer(options)
            parse_normalizer(options)
            options
          end

          def parse_flags(options)
            options[:type] = :hash
            if @command.table_no_key?
              options[:type] = :array
            elsif @command.table_hash_key?
              options[:type] = :hash
            elsif @command.table_pat_key?
              options[:type] = :patricia_trie
            elsif @command.table_dat_key?
              options[:type] = :double_array_trie
            end
            if @command.key_with_sis? and @command.table_pat_key?
              options[:key_with_sis] = true
            end
          end

          def parse_key_type(options)
            return unless @command["key_type"]
            options[:key_type] = @command["key_type"]
          end

          def parse_value_type(options)
            return unless @command["value_type"]
            options[:value_type] = @command["value_type"]
          end

          def parse_default_tokenizer(options)
            return unless @command["default_tokenizer"]
            options[:default_tokenizer] = @command["default_tokenizer"]
          end

          def parse_normalizer(options)
            return unless @command["normalizer"]
            options[:normalizer] = @command["normalizer"]
          end
        end

        class Handler < Droonga::Handler
          action.synchronous = true

          def handle(message)
            command = Command.new(@context)
            command.execute(message.request)
          end
        end

        Groonga.define_single_step do |step|
          step.name = "table_create"
          step.write = true
          step.handler = Handler
          step.collector = Collectors::Or
        end
      end
    end
  end
end
