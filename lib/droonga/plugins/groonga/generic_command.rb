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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "groonga"

module Droonga
  module Plugins
    module Groonga
      module Status
        SUCCESS          = 0
        INVALID_ARGUMENT = -22
        SYNTAX_ERROR     = -63
      end

      class GenericCommand
        class CommandError < StandardError
          attr_reader :status, :message, :result

          def initialize(params={})
            @status = params[:status]
            @message = params[:message]
            @result = params[:result]
          end
        end

        def initialize(context)
          @context = context
        end

        def execute(request)
          @start_time = Time.now.to_f
          result = process_request(request)
          [header(Status::SUCCESS), result]
        rescue CommandError => error
          unless error.result.nil?
            [header(error.status, error.message), error.result]
          else
            [header(error.status, error.message)]
          end
        end

        private
        def header(return_code, error_message="")
          elapsed_time = Time.now.to_f - @start_time
          header = [return_code, @start_time, elapsed_time]
          header.push(error_message) unless error_message.empty?
          header
        end

        def valid_table_name(name, params={})
          error_result = params[:error_result]
          table_name = @command[name]

          if table_name.nil?
            message = "you must specify table via \"#{name}\""
            raise CommandError.new(:status => Status::INVALID_ARGUMENT,
                                   :message => message,
                                   :result => error_result)
          end

          if @context[table_name].nil?
            message = "table not found: <#{table_name.to_s}>"
            raise CommandError.new(:status => Status::INVALID_ARGUMENT,
                                   :message => message,
                                   :result => error_result)
          end

          table_name
        end

        def valid_column_name(name, params={})
          table_name = params[:table_name]
          error_result = params[:error_result]
          column_name = @command[name]

          if column_name.nil?
            message = "you must specify column via \"#{name}\""
            raise CommandError.new(:status => Status::INVALID_ARGUMENT,
                                   :message => message,
                                   :result => error_result)
          end

          if @context[table_name].column(column_name).nil?
            message = "column not found: <#{column_name.to_s}> in " +
                          "<#{table_name.to_s}>"
            raise CommandError.new(:status => Status::INVALID_ARGUMENT,
                                   :message => message,
                                   :result => error_result)
          end

          column_name
        end

        # After schema changes, we must restart workers, because
        # old schema information cached by workers can break
        # indexes for newly added records.
        def restart_workers
          #XXX IMPLEMENT ME!!
        end
      end
    end
  end
end
