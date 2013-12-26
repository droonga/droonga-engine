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
require "droonga/message_processing_error"

module Droonga
  class AddHandler < Droonga::HandlerPlugin
    repository.register("add", self)

    class MissingTableParameter < BadRequest
      def initialize
        super("\"table\" must be specified.")
      end
    end

    class MissingPrimaryKeyParameter < BadRequest
      def initialize(table_name)
        super("\"key\" must be specified. " +
                "The table #{table_name.inspect} requires a primary key for a new record.")
      end
    end

    class UnknownTable < BadRequest
      def initialize(table_name)
        super("The table #{table_name.inspect} does not exist in the dataset.")
      end

      def status_code
        404
      end
    end

    command :add
    def add(message, messenger)
      outputs = process_add(message.request)
      messenger.emit(outputs)
    end

    private
    def process_add(request)
      raise MissingTableParameter.new unless request.include?("table")

      table = @context[request["table"]]
      raise UnknownTable.new(request["table"]) unless table

      if table.support_key?
        unless request.include?("key")
          raise MissingPrimaryKeyParameter.new(request["table"])
        end
        table.add(request["key"], request["values"])
      else
        table.add(request["values"])
      end
      [true]
    end
  end
end
