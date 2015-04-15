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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "socket"

require "droonga/client"

require "droonga/node_name"
require "droonga/catalog/dataset"

module Droonga
  module Catalog
  class Fetcher
    class EmptyResponse < StandardError
    end

    class EmptyCatalog < StandardError
    end

    def initialize(client_options)
      @client_options = default_options.merge(client_options)
    end

    def fetch(options={})
      message = {
        "dataset" => options[:dataset] || Catalog::Dataset::DEFAULT_NAME,
        "type"    => "catalog.fetch"
      }
      response = nil
      Droonga::Client.open(@client_options) do |client|
        response = client.request(message)
      end
      raise EmptyResponse.new unless response
      raise EmptyCatalog.new unless response["body"]
      response["body"]
    end

    private
    def default_options
      {
        :host          => "127.0.0.1",
        :port          => NodeName::DEFAULT_PORT,
        :tag           => NodeName::DEFAULT_TAG,
        :protocol      => :droonga,
        :timeout       => 1,
        :receiver_host => Socket.gethostname,
        :receiver_port => 0,
      }
    end
  end
  end
end
