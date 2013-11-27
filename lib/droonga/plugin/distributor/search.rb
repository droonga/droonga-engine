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

        final_offset, final_limit = calculate_offset_and_limit!(query)

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
              "offset" => final_offset,
              "limit" => final_limit,
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

    private
    UNLIMITED = -1

    def calculate_offset_and_limit!(query)
      rich_sort = query["sortBy"].is_a?(Hash)

      # offset for workers must be zero.
      sort_offset = 0
      if rich_sort
        sort_offset = query["sortBy"]["offset"] || 0
        query["sortBy"]["offset"] = 0
      end

      output_offset = query["output"]["offset"] || 0
      query["output"]["offset"] = 0

      final_offset = sort_offset + output_offset

      # we have to calculate limit based on offset.
      # <A, B = limited integer (0...MAXINT)>
      # | sort      | output    | => | worker's sort limit      | worker's output limit | final limit |
      # =========================    ==================================================================
      # | UNLIMITED | UNLIMITED | => | UNLIMITED                | UNLIMITED             | UNLIMITED   |
      # | UNLIMITED | B         | => | final_offset + B         | UNLIMITED             | B           |
      # | A         | UNLIMITED | => | final_offset + A         | UNLIMITED             | A           |
      # | A         | B         | => | final_offset + min(A, B) | UNLIMITED             | min(A, B)   |
      sort_limit = UNLIMITED
      if rich_sort
        sort_limit = query["sortBy"]["limit"] || UNLIMITED
      end
      output_limit = query["output"]["limit"] || 0
      query["output"]["limit"] = UNLIMITED

      final_limit = 0
      if sort_limit == UNLIMITED && output_limit == UNLIMITED
        final_limit = UNLIMITED
      else
        if sort_limit == UNLIMITED
          final_limit = output_limit
        elsif output_limit == UNLIMITED
          final_limit = sort_limit
        else
          final_limit = [sort_limit, output_limit].min
        end
        query["sortBy"]["limit"] = final_offset + final_limit if rich_sort
      end

      [final_offset, final_limit]
    end
  end
end
