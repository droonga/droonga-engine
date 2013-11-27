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

require "droonga/distributor_plugin"

module Droonga
  class SearchDistributor < Droonga::DistributorPlugin
    repository.register("search", self)

    command :search
    def search(envelope)
      message = []
      input_names = []
      output_names = []
      name_mapper = {}
      request = envelope["body"]
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
            elements[element] = {
              "type" => "sum",
            }
          when "records"
            # TODO: must take "sortBy" section into account.
            elements[element] = {
              "type" => "sort",
              "order" => ["<"],
            }
          end
        end
        reducer = {
          "inputs"=> [input_name],
          "outputs"=> [output_name],
          "type"=> "reduce",
          "body"=> {
            input_name=> {
              output_name=> elements,
            },
          },
        }
        message << reducer
      end
      gatherer = {
        "inputs"=> output_names,
        "type"=> "gather",
        "body"=> name_mapper,
        "post"=> true,
      }
      message << gatherer
      searcher = {
        "dataset"=> envelope["dataset"] || request["dataset"],
        "outputs"=> input_names,
        "type"=> "broadcast",
        "command"=> "search",
        "replica"=> "random",
        "body"=> request,
      }
      message.push(searcher)
      post(message)
    end
  end
end
