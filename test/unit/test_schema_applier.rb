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

require "droonga/schema_applier"

class SchemaCreatorTest < Test::Unit::TestCase
  include Sandbox

  def setup
    setup_database
    @context = Groonga::Context.default
  end

  def teardown
    teardown_database
  end

  def apply(schema_data)
    schema = Droonga::Catalog::Schema.new("dataset", schema_data)
    applier = Droonga::SchemaApplier.new(@context, schema)
    applier.apply
  end

  def dump
    dumper = Groonga::SchemaDumper.new(:context => @context, :syntax => :command)
    dumper.dump
  end

  def test_reference_table
    schema_data = {
      "Users" => {
        "type" => "Hash",
        "keyType" => "Names",
      },
      "Names" => {
        "type" => "Hash",
        "keyType" => "ShortText",
      },
    }
    apply(schema_data)
    assert_equal(<<-DUMP, dump)
table_create Names TABLE_HASH_KEY ShortText

table_create Users TABLE_HASH_KEY Names
    DUMP
  end
end
