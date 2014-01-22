# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Droonga Project
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

require "droonga/handler_plugin"

module Droonga
  class GroongaHandler < Droonga::HandlerPlugin
    repository.register("groonga", self)

    command :table_create
    def table_create(message, messenger)
      command = TableCreate.new(@context)
      outputs = command.execute(message.request)
      messenger.emit(outputs)
    end

    command :table_remove
    def table_remove(message, messenger)
      command = TableRemove.new(@context)
      outputs = command.execute(message.request)
      messenger.emit(outputs)
    end

    command :column_create
    def column_create(message, messenger)
      command = ColumnCreate.new(@context)
      outputs = command.execute(message.request)
      messenger.emit(outputs)
    end

    def prefer_synchronous?(command)
      return true
    end

    module Status
      SUCCESS          = 0
      INVALID_ARGUMENT = -22
    end

    class Command
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
        [header(error.status, error.message), error.result]
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

require "droonga/plugin/handler/groonga/table_create"
require "droonga/plugin/handler/groonga/table_remove"
require "droonga/plugin/handler/groonga/column_create"
