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
    attr_reader :outputs

    def initialize(source_message)
      @source_message = source_message

      @key = nil
      @dataset = nil
      @outputs = []

      @reducers = []
      @gatherers = []
      @processors = []

      plan_errors_handling
    end

    def messages
      @reducers + @gatherers + @processors
    end

    def reduce(name, reducer)
      @reducers << reducer_message("reduce", name, reducer)
      @gatherers << gatherer_message("gather", name)
    end

    def scatter_all(body=nil)
      raise MessageProcessingError.new("missing key") unless @key
      @processors << {
        "command" => @source_message["type"],
        "dataset" => @dataset || @source_message["dataset"],
        "body"    => body || @source_message["body"],
        "key"     => @key,
        "type"    => "scatter",
        "outputs" => @outputs,
        "replica" => "all",
        "post"    => true
      }
    end

    def broadcast_all(body=nil)
      @processors << {
        "command" => @source_message["type"],
        "dataset" => @dataset || @source_message["dataset"],
        "body"    => body || @source_message["body"],
        "type"    => "broadcast",
        "outputs" => @outputs,
        "replica" => "all",
        "post"    => true
      }
    end

    def broadcast_at_random(body=nil)
      @processors << {
        "command" => @source_message["type"],
        "dataset" => @dataset || @source_message["dataset"],
        "body"    => body || @source_message["body"],
        "type"    => "broadcast",
        "outputs" => @outputs,
        "replica" => "random",
      }
    end

    private
    def reducer_message(command, name, reducer)
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

    def gatherer_message(command, name)
      {
        "type"   => command,
        "body"   => {
          output_name(name) => {
            "output" => name,
          },
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
      @outputs << "errors"
      reduce("errors", "type" => "sum", "limit" => -1)
    end
  end
end
