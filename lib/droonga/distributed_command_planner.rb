# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Droonga Project
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

module Droonga
  class DistributedCommandPlanner
    attr_accessor :key, :dataset

    REDUCE_SUM = "sum"

    DEFAULT_LIMIT = -1

    def initialize(source_message)
      @source_message = source_message

      @key = nil
      @dataset = nil
      @outputs = []

      @reducers = []
      @gatherers = []
      @processor = nil

      plan_errors_handling
    end

    def plan
      unified_reducers + unified_gatherers + [fixed_processor]
    end

    def reduce(params=nil)
      return unless params
      params.each do |name, reducer|
        gatherer = nil
        if reducer.is_a?(Hash) and reducer[:gather]
          gatherer = reducer[:gather]
          reducer = reducer[:reduce]
        end
        @reducers << reducer_message(reduce_command, name, reducer)
        @gatherers << gatherer_message(gather_command, name, gatherer)
        @outputs << name
      end
    end

    def scatter(options={})
      @processor = {
        "command" => @source_message["type"],
        "dataset" => @dataset || @source_message["dataset"],
        "body"    => options[:body] || @source_message["body"],
        "key"     => nil,
        "type"    => "scatter",
        "outputs" => [],
        "replica" => "all",
        "post"    => true
      }
    end

    def broadcast(options={})
      processor = {
        "command" => @source_message["type"],
        "dataset" => @dataset || @source_message["dataset"],
        "body"    => options[:body] || @source_message["body"],
        "type"    => "broadcast",
        "outputs" => [],
        "replica" => "random"
      }
      if options[:write]
        processor["replica"] = "all"
        processor["post"]    = true
      end
      @processor = processor
    end

    private
    def reduce_command
      "reduce"
    end

    def gather_command
      "gather"
    end

    def unified_reducers
      unified_reducers = {}
      @reducers.each do |reducer|
        type = reducer["type"]
        unified = unified_reducers[type]
        if unified
          unified["body"] = unified["body"].merge(reducer["body"])
          unified["inputs"] = unified["inputs"] + reducer["inputs"]
          unified["outputs"] = unified["outputs"] + reducer["outputs"]
        else
          unified_reducers[type] = Marshal.load(Marshal.dump(reducer))
        end
      end
      unified_reducers.values
    end

    def unified_gatherers
      unified_gatherers = {}
      @gatherers.each do |gatherer|
        type = gatherer["type"]
        unified = unified_gatherers[type]
        if unified
          unified["body"] = unified["body"].merge(gatherer["body"])
          unified["inputs"] = unified["inputs"] + gatherer["inputs"]
        else
          unified_gatherers[type] = Marshal.load(Marshal.dump(gatherer))
        end
      end
      unified_gatherers.values
    end

    def fixed_processor
      @processor["outputs"] = @outputs
      if @processor["type"] == "scatter"
        raise MessageProcessingError.new("missing key") unless @key
        @processor["key"] = @key
      end
      @processor
    end

    def reducer_message(command, name, reducer)
      if reducer.is_a?(String)
        reducer = {
          "type" => reducer,
        }
        if reducer["type"] == REDUCE_SUM
          reducer["limit"] = DEFAULT_LIMIT
        end
      end
      {
        "type"    => command,
        "body"    => {
          name => {
            output_name(name) => reducer,
          },
        },
        "inputs"  => [name],
        "outputs" => [output_name(name)],
      }
    end

    def gatherer_message(command, name, gatherer=nil)
      gatherer ||= {}
      {
        "type"   => command,
        "body"   => {
          output_name(name) => {
            "output" => name,
          }.merge(gatherer),
        },
        "inputs" => [output_name(name)],
        "post"   => true,
      }
    end

    def output_name(name)
      "#{name}_reduced"
    end

    #XXX Now, we include definitions to merge errors in the body.
    #    However, this makes the term "errors" reserved, so plugins
    #    cannot use their custom "errors" in the body.
    #    This must be rewritten. 
    def plan_errors_handling
      reduce("errors"=> REDUCE_SUM)
    end
  end
end
