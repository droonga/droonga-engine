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
    attr_accessor :key
    attr_reader :outputs

    def initialize(source_message)
      @source_message = source_message

      @key = nil
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
      @reducers << {
        "type"    => "reduce",
        "body"    => {
          name => {
            "#{name}_reduced" => reducer,
          },
        },
        "inputs"  => [name],
        "outputs" => ["#{name}_reduced"],
      }

      @gatherers << {
        "type"   => "gather",
        "body"   => {
          "#{name}_reduced" => {
            "output" => name,
          },
        },
        "inputs" => ["#{name}_reduced"],
        "post"   => true,
      }
    end

    def scatter_all
      raise MessageProcessingError.new("missing key") unless @key
      @processors << {
        "command" => @source_message["type"],
        "dataset" => @source_message["dataset"],
        "body"    => @source_message["body"],
        "key"     => @key,
        "type"    => "scatter",
        "outputs" => @outputs + ["errors"],
        "replica" => "all",
        "post"    => true
      }
    end

    def broadcast_all
      @processors << {
        "command" => @source_message["type"],
        "dataset" => @source_message["dataset"],
        "body"    => @source_message["body"],
        "type"    => "broadcast",
        "outputs" => @outputs + ["errors"],
        "replica" => "all",
        "post"    => true
      }
    end

    private
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
