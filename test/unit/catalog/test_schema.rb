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

require "droonga/catalog/schema"

class CatalogSchemaTest < Test::Unit::TestCase
  private
  def create_schema(data)
    Droonga::Catalog::Schema.new(data)
  end

  class SchemaTest < self
    def test_schema_not_specified
      assert_equal([],
                   create_schema(nil).tables)
    end

    def test_no_table
      assert_equal([],
                   create_schema({}).tables)
    end

    def test_tables
      assert_equal(
        [Droonga::Catalog::Schema::Table.new('table1', {})],
        create_schema(
          "table1" => {
        }).tables
      )
    end
  end
end
