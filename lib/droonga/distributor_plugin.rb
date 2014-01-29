# -*- coding: utf-8 -*-
#
# Copyright (C) 2013-2014 Droonga Project
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

require "droonga/plugin"

module Droonga
  class DistributorPlugin < Plugin
    extend PluginRegisterable

    def initialize(distributor)
      super()
      @distributor = distributor
    end

    def distribute(messages)
      @distributor.distribute(messages)
    end

    def scatter_all(message, key)
      messages = [reducer(message), gatherer(message), scatterer(message, key)]
      distribute(messages)
    end

    def broadcast_all(message)
      messages = [reducer(message), gatherer(message), broadcaster(message)]
      distribute(messages)
    end

    private
    def process_error(command, error, arguments)
      if error.is_a?(MessageProcessingError)
        raise error
      else
        super
      end
    end

    #XXX Now, default scatterer/broadcaster/reducer/gatherer includes
    #    definitions to merge errors in the body. However, this makes
    #    the term "errors" reserved, so plugins cannot use their custom
    #    "errors" in the body. This must be rewritten. 

    def scatterer(message, key)
      {
        "command" => message["type"],
        "dataset" => message["dataset"],
        "body"    => message["body"],
        "key"     => key,
        "type"    => "scatter",
        "outputs" => ["errors"],
        "replica" => "all",
        "post"    => true
      }
    end

    def broadcaster(message)
      {
        "command" => message["type"],
        "dataset" => message["dataset"],
        "body"    => message["body"],
        "type"    => "broadcast",
        "outputs" => ["errors"],
        "replica" => "all",
        "post"    => true
      }
    end

    def reducer(message)
      {
        "type"    => "reduce",
        "body"    => {
          "errors" => {
            "errors_reduced" => {
              "type" => "sum",
              "limit" => -1,
            },
          },
        },
        "inputs"  => ["errors"],
        "outputs" => ["errors_reduced"],
      }
    end

    def gatherer(message)
      {
        "type"   => "gather",
        "body"   => {
          "errors_reduced" => {
            "output" => "errors",
          },
        },
        "inputs" => ["errors_reduced"],
        "post"   => true,
      }
    end
  end
end
