# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 droonga project
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

require "time"

require 'groonga'

module Droonga
  class Worker
    def initialize(database, queue_name)
      @context = Groonga::Context.new
      @database = @context.open_database(database)
      @queue_name = queue_name
    end

    def shutdown
      @database.close
      @context.close
      @database = @context = nil
    end

    def process_message(envelope)
      case envelope["type"]
      when "search"
        search(envelope["body"])
      end
    end

    private
    def search(request)
      result = {}
      request["queries"].each do |name, query|
        result[name] = search_query(query)
      end
      result
    end

    def search_query(query)
      start_time = Time.now
      source = @context[query["source"]]
      columns = source.columns
      attributes = columns.collect do |column|
        {
          "name" => column.local_name,
          "type" => column.range.name,
          "vector" => column.vector?,
        }
      end
      column_names = columns.collect(&:local_name)
      records = source.collect do |record|
        column_names.collect do |name|
          record[name]
        end
      end
      elapsed_time = Time.now.to_f - start_time.to_f

      {
        "count" => source.size,
        "startTime" => start_time.iso8601,
        "elapsedTime" => elapsed_time,
        "attributes" => attributes,
        "records" => records,
      }
    end
  end
end
