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

require "droonga/catalog"

class CatalogTest < Test::Unit::TestCase
  def setup
    @catalog = Droonga::Catalog.new(catalog_path)
  end

  def test_option
    assert_equal(["for_global"], @catalog.option("plugins"))
  end

  def test_get_partitions
    partitions = @catalog.get_partitions("localhost:23003/test")
    base_path = File.expand_path("../fixtures", __FILE__)
    assert_equal({
                   "localhost:23003/test.000" => {
                     :database  => "#{base_path}/000/db",
                     :handlers  => ["for_dataset"],
                     :n_workers => 0
                   },
                   "localhost:23003/test.001" => {
                     :database  => "#{base_path}/001/db",
                     :handlers  => ["for_dataset"],
                     :n_workers => 0
                   },
                   "localhost:23003/test.002" => {
                     :database  => "#{base_path}/002/db",
                     :handlers  => ["for_dataset"],
                     :n_workers => 0
                   },
                   "localhost:23003/test.003" => {
                     :database  => "#{base_path}/003/db",
                     :handlers  => ["for_dataset"],
                     :n_workers => 0
                   },
                 },
                 partitions)
  end

  private
  def catalog_path
    @catalog_path ||= File.expand_path("../fixtures/catalog.json", __FILE__)
  end
end
