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

require "droonga/catalog_generator"
require "droonga/client"

module Droonga
  class CatalogFetcher
    class << self
      def fetch(client_options)
        new(client_options).fetch
      end
    end

    def initialize(client_options)
      @client_options = default_options.merge(client_options)
    end

    def fetch
      catalog = nil
      Droonga::Client.open(@client_options) do |client|
        response = client.request(:dataset => @client_options[:dataset],
                                  :type    => "catalog.fetch")
        catalog = response["body"]
      end
      catalog
    end

    private
    def default_options
      {
        :dataset       => CatalogGenerator::DEFAULT_DATASET,
        :host          => "127.0.0.1",
        :port          => CatalogGenerator::DEFAULT_PORT,
        :tag           => CatalogGenerator::DEFAULT_TAG,
        :protocol      => :droonga,
        :timeout       => 1,
        :receiver_host => "127.0.0.1",
        :receiver_port => 0,
      }
    end
  end
end
