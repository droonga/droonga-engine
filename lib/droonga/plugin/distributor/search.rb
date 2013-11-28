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
      output_mapper = {}
      request = envelope["body"]
      request["queries"].each do |input_name, query|
        output = query["output"]
        next unless output

        input_names << input_name
        output_name = input_name + "_reduced"
        output_names << output_name
        output_mapper[output_name] = {
          "source" => input_name,
        }

        # override the format, because the collector can/should handle only array type records...
        final_format = output["format"] || "simple"
        output["format"] = "simple"

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
            final_attributes = output["attributes"]
            if final_attributes.is_a?(Hash)
              final_attributes = final_attributes.keys
            else
              final_attributes.collect! do |attribute|
                if attribute.is_a?(Hash)
                  attribute["label"] || attribute["source"]
                else
                  attribute
                end
              end
            end
            elements[element] = sort_reducer(:attributes => output["attributes"],
                                             :sort_keys => query["sortBy"])
            output_mapper[output_name]["element"] = element
            output_mapper[output_name]["offset"] = final_offset
            output_mapper[output_name]["limit"] = final_limit
            output_mapper[output_name]["format"] = final_format
            output_mapper[output_name]["attributes"] = final_attributes
          end
        end

        reducer = {
          "inputs" => [input_name],
          "outputs" => [output_name],
          "type" => "reduce",
          "body" => {
            input_name => {
              output_name => elements,
            },
          },
        }
        message << reducer
      end
      gatherer = {
        "type" => "gather",
        "body" => output_mapper,
        "inputs" => output_names,
        "post" => true,
      }
      message << gatherer
      searcher = {
        "dataset" => envelope["dataset"] || request["dataset"],
        "outputs" => input_names,
        "type" => "broadcast",
        "command" => "search",
        "replica" => "random",
        "body" => request,
      }
      message.push(searcher)
      post(message)
    end

    private
    UNLIMITED = -1

    def calculate_offset_and_limit!(query)
      rich_sort = query["sortBy"].is_a?(Hash)

      have_records = false
      if query["output"] &&
           query["output"]["elements"].is_a?(Array) &&
           query["output"]["elements"].include?("records")
        have_records = true
      end

      # offset for workers must be zero.
      sort_offset = 0
      if rich_sort
        sort_offset = query["sortBy"]["offset"] || 0
        query["sortBy"]["offset"] = 0
      end

      output_offset = query["output"]["offset"] || 0
      query["output"]["offset"] = 0 if have_records

      final_offset = sort_offset + output_offset

      # we have to calculate limit based on offset.
      # <A, B = limited integer (0...MAXINT)>
      # | sort      | output    | => | worker's sort limit      | worker's output limit   | final limit |
      # =========================    ====================================================================
      # | UNLIMITED | UNLIMITED | => | UNLIMITED                | UNLIMITED               | UNLIMITED   |
      # | UNLIMITED | B         | => | final_offset + B         | final_offset + B        | B           |
      # | A         | UNLIMITED | => | final_offset + A         | final_offset + A        | A           |
      # | A         | B         | => | final_offset + min(A, B) | final_offset + min(A, B)| min(A, B)   |
      sort_limit = UNLIMITED
      if rich_sort
        sort_limit = query["sortBy"]["limit"] || UNLIMITED
      end
      output_limit = query["output"]["limit"] || 0

      final_limit = 0
      if sort_limit == UNLIMITED && output_limit == UNLIMITED
        final_limit = UNLIMITED
        query["output"]["limit"] = UNLIMITED
      else
        if sort_limit == UNLIMITED
          final_limit = output_limit
        elsif output_limit == UNLIMITED
          final_limit = sort_limit
        else
          final_limit = [sort_limit, output_limit].min
        end
        query["sortBy"]["limit"] = final_offset + final_limit if rich_sort
        query["output"]["limit"] = final_offset + final_limit
      end

      [final_offset, final_limit]
    end

    def sort_reducer(params)
      attributes = params[:attributes]
      sort_keys = params[:sort_keys]
      sort_keys = sort_keys["keys"] if sort_keys.is_a?(Hash)

      order = []
#      unless sort_keys
        order << "<"
#      else
#        # XXX NOT IMPLEMENTED YET!
#        # we must change the format of "order" from array to hash (rich object)
#      end

      {
        "type" => "sort",
        "order" => order,
      }
    end
  end
end
