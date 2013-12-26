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
  class AddHandler < Droonga::HandlerPlugin
    repository.register("add", self)

    class InvalidRequest < Droonga::HandlerError::HandlerClientError
    end

    class MissingTable < InvalidRequest
      def initialize(options={})
        super("\"table\" must be specified.", options)
      end
    end

    class MissingPrimaryKey < InvalidRequest
      def initialize(table_name, options={})
        super("\"key\" must be specified. " +
                "The table #{table_name.inspect} requires a primary key for a new record.",
              options)
      end
    end

    class UnknownTable < InvalidRequest
      def initialize(table_name, options={})
        super("The table #{table_name.inspect} does not exist in the dataset.",
              options)
      end
    end

    command :add
    def add(message, messenger)
      outputs = process_add(message.request)
      messenger.emit(outputs)
    end

    private
    def process_add(request)
      raise MissingTable.new unless request.include?("table")

      table = @context[request["table"]]
      raise UnknownTable.new(request["table"]) unless table

      if table.support_key?
        raise MissingPrimaryKey.new(request["table"]) unless request.include?("key")
        table.add(request["key"], request["values"])
      else
        table.add(request["values"])
      end
      [true]
    end
  end
end
