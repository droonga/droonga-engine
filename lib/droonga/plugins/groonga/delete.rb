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

require "groonga/command/delete"

require "droonga/plugin"
require "droonga/plugins/groonga/generic_command"

module Droonga
  module Plugins
    module Groonga
      module Delete
        class Command < GenericCommand
          def process_request(request)
            command_class = ::Groonga::Command.find("delete")
            @command = command_class.new("delete", request)

            table_name = @command["table"]
            key = @command["key"]
            id = @command["id"]
            filter = @command["filter"]

            validate_parameters(table_name, key, id, filter)

            table = @context[table_name]
            if key
              delete_record_by_key(table, key)
            elsif id
              delete_record_by_id(table, id)
            else
              delete_record_by_filter(table, filter)
            end

            true
          end

          private
          def validate_parameters(table_name, key, id, filter)
            if table_name.nil? or @context[table_name].nil?
              message = "table doesn't exist: <#{table_name}>"
              raise CommandError.new(:status => Status::INVALID_ARGUMENT,
                                     :message => message,
                                     :result => false)
            end

            if key.nil? and id.nil? and filter.nil?
              message = "you must specify \"key\", \"id\", or \"filter\""
              raise CommandError.new(:status => Status::INVALID_ARGUMENT,
                                     :message => message,
                                     :result => false)
            end

            count = 0
            count += 1 if key
            count += 1 if id
            count += 1 if filter
            if count > 1
              message = "\"key\", \"id\", and \"filter\" are exclusive"
              raise CommandError.new(:status => Status::INVALID_ARGUMENT,
                                     :message => message,
                                     :result => false)
            end
          end

          def delete_record_by_key(table, key)
            record = table[key]
            record.delete unless record.nil?
          end

          def delete_record_by_id(table, id)
            record = table[id.to_i]
            record.delete if record and record.valid_id?
          end

          def delete_record_by_filter(table, filter)
            condition = ::Groonga::Expression.new(:context => @context)
            condition.define_variable(:domain => table)
            begin
              condition.parse(filter, :syntax => :script)
            rescue ::Groonga::SyntaxError
              message = "syntax error in filter: <#{filter}>"
              raise CommandError.new(:status => Status::SYNTAX_ERROR,
                                     :message => message,
                                     :result => false)
            end
            records = table.select(condition)
            records.each do |record|
              record.key.delete
            end
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
          step.name = "delete"
          step.write = true
          step.handler = Handler
          step.collector = Collectors::Or
        end
      end
    end
  end
end
