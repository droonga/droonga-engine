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

require "fileutils"

require "droonga/plugins/catalog"
require "droonga/path"

class CatalogFetchHandlerTest < Test::Unit::TestCase
  def setup
    setup_handler
    setup_catalog_json
  end

  def teardown
    teardown_handler
    teardown_catalog_json
  end

  private
  def setup_handler
    @worker = StubWorker.new
    @messenger = Droonga::Test::StubHandlerMessenger.new
    @loop = nil
    @handler = Droonga::Plugins::Catalog::FetchHandler.new("name",
                                                           @worker.context,
                                                           @messenger,
                                                           @loop)
  end

  def teardown_handler
    @handler = nil
  end

  def setup_catalog_json
    catalog_path = Droonga::Path.catalog
    FileUtils.mkdir_p(catalog_path.parent.to_s)
    catalog_path.open("w") do |file|
      file.puts(JSON.generate(catalog))
    end
  end

  def teardown_catalog_json
    FileUtils.rm_f(Droonga::Path.catalog)
  end

  def catalog
    {
      "version" => 2,
    }
  end

  def process(request)
    message = Droonga::Test::StubHandlerMessage.new(request)
    @handler.handle(message)
  end

  public
  def test_request
    request = {}
    response = process(request)
    assert_equal(catalog, response)
  end
end
