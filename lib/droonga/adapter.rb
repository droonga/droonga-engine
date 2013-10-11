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

require "droonga/handler"

module Droonga
  class Adapter < Droonga::Handler
    def scatter_all(request, key)
      message = [{
        "command"=> envelope["type"],
        "dataset"=> envelope["dataset"],
        "body"=> request,
        "key"=> key,
        "type"=> "scatter",
        "replica"=> "all",
        "post"=> true
      }]
      post(message, "proxy")
    end

    def broadcast_all(request)
      message = [{
        "command"=> envelope["type"],
        "dataset"=> envelope["dataset"],
        "body"=> request,
        "type"=> "broadcast",
        "replica"=> "all",
        "post"=> true
      }]
      post(message, "proxy")
    end

    def prefer_synchronous?(command)
      return true
    end
  end

  class BasicAdapter < Adapter
    Droonga::HandlerPlugin.register("adapter", self)

    command :table_create
    def table_create(request)
      broadcast_all(request)
    end

    command :column_create
    def column_create(request)
      broadcast_all(request)
    end

    command "watch.feed" => :feed
    def feed(request)
      puts "adapter received #{request}"
      broadcast_all(request)
    end

    command "watch.subscribe" => :subscribe
    def subscribe(request)
      puts "adapter received #{request}"
      broadcast_all(request)
    end

    command "watch.unsubscribe" => :unsubscribe
    def unsubscribe(request)
      puts "adapter received #{request}"
      broadcast_all(request)
    end

    command :add
    def add(request)
      # TODO: update events must be serialized in the primary node of replicas.
      key = request["key"] || rand.to_s
      scatter_all(request, key)
    end

    command :update
    def update(request)
      # TODO: update events must be serialized in the primary node of replicas.
      key = request["key"] || rand.to_s
      scatter_all(request, key)
    end

    command :reset
    def add(request)
      # TODO: update events must be serialized in the primary node of replicas.
      key = request["key"] || rand.to_s
      scatter_all(request, key)
    end

    command :search
    def search(request)
      message = []
      input_names = []
      output_names = []
      name_mapper = {}
      request["queries"].each do |input_name, query|
        output = query["output"]
        next unless output
        input_names << input_name
        output_name = input_name + "_reduced"
        output_names << output_name
        name_mapper[output_name] = input_name
        # TODO: offset & limit must be arranged here.
        elements = {}
        output["elements"].each do |element|
          case element
          when "count"
            elements[element] = ["sum"]
          when "records"
            # TODO: must take "sortBy" section into account.
            elements[element] = ["sort", "<"]
          end
        end
        reducer = {
          "inputs"=> [input_name],
          "outputs"=> [output_name],
          "type"=> "reduce",
          "body"=> {
            input_name=> {
              output_name=> elements
            }
          }
        }
        message << reducer
      end
      gatherer = {
        "inputs"=> output_names,
        "type"=> "gather",
        "body"=> name_mapper,
        "post"=> true
      }
      message << gatherer
      searcher = {
        "dataset"=> envelope["dataset"] || request["dataset"],
        "outputs"=> input_names,
        "type"=> "broadcast",
        "command"=> "search",
        "replica"=> "random",
        "body"=> request
      }
      message.push(searcher)
      post(message, "proxy")
    end
  end
end
