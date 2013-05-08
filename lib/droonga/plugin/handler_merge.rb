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
  class MergeHandler < Droonga::Handler
    Droonga::HandlerPlugin.register("merge", self)

    CONFIG_FILE_PATH = 'config.json'

    def initialize(*arguments)
      super
      open(CONFIG_FILE_PATH) do |file|
        @config = JSON.parse(file.read)
      end
      @mergers = {}
    end

    command "merge" => :adapt_request
    command "merge.result" => :adapt_reply

    def adapt_request(request, *arguments)
      dataset = @config["datasets"][request["dataset"]]
      return unless dataset
      @mergers[envelope["id"]] = merger = Merger.new(dataset)
      add_route(merger.merger_path)
      merger.routes.each do |route|
        post(request, route)
      end
    end

    def adapt_reply(reply)
      id = envelope["id"]
      merger = @mergers[id]
      return unless merger
      merger.add(reply)
      return unless merger.fulfilled?
      post(merger.result)
      @mergers.delete(id)
    end

    class Merger
      attr_reader :routes
      attr_reader :result
      attr_reader :merger_path
      def initialize(dataset)
        @dataset = dataset
        @merge_policy = dataset["merge_policy"]
        @merger_path = dataset["merger_path"] || "merge.result"
        @routes = []
        dataset["shards"].collect do |key, shard|
          n_replications = shard["instances"].size
          next if n_replications.zero?
          index = rand(n_replications)
          @routes << shard["instances"][index]["route"]
        end
        @n_shards = @routes.size
        @n_replies = 0
        @result = nil
      end

      def add(reply)
        if @result
          merge!(@result, reply)
        else
          @result = reply
        end
        @n_replies += 1
      end

      def fulfilled?()
        @n_replies == @n_shards
      end

      private
      def merge!(a, b)
        @merge_policy.each do |policy|
          path = policy["path"]
          case policy["procedure"]
          when "sum"
            last = path[-1]
            _a, _b = fetch_element(path[0..-2], a, b)
            _a[last] += _b[last]
          when "sort"
            _a, _b = fetch_element(path, a, b)
            merge_sort!(_a, _b, policy["order"])
          end
        end
      end

      def fetch_element(path, a, b)
        path.each do |index|
          a = a[index]||a[index]
          b = b[index]||b[index]
        end
        [a, b]
      end

      def compare(a, b, operators)
        for index in 0..a.size-1 do
          _a = a[index]
          _b = b[index]
          operator = operators[index]
          break unless operator
          return true if _a.__send__(operator, _b)
        end
        return false
      end

      def merge_sort!(a, b, order)
        index = 0
        b.each do |_b|
          loop do
            _a = a[index]
            break unless _a
            break if compare(_b, _a, order)
            index += 1
          end
          a.insert(index, _b)
          index += 1
        end
      end
    end
  end
end
