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
      end
    end
  end
end
