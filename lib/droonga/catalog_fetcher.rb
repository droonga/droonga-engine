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

require "droonga/client"

require "droonga/address"
require "droonga/catalog/dataset"

module Droonga
  class CatalogFetcher
    def initialize(client_options)
      @client_options = default_options.merge(client_options)
    end

    def fetch(options={})
      message = {
        "dataset" => options[:dataset] || Catalog::Dataset::DEFAULT_NAME,
        "type"    => "catalog.fetch"
      }
      Droonga::Client.open(@client_options) do |client|
        response = client.request(message)
        response["body"]
      end
    end

    private
    def default_options
      {
        :host          => "127.0.0.1",
        :port          => Address::DEFAULT_PORT,
        :tag           => Address::DEFAULT_TAG,
        :protocol      => :droonga,
        :timeout       => 1,
        :receiver_host => "127.0.0.1",
        :receiver_port => 0,
      }
    end
  end
end
