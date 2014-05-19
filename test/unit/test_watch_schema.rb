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

require "helper"

require "droonga/watch_schema"

class WatchSchemaTest < Test::Unit::TestCase
  def setup
    @database_path = @temporary_directory + "droonga/watch/db"
    @context = Groonga::Context.default
    FileUtils.mkdir_p(File.dirname(@database_path))
    @context.create_database(@database_path.to_s)
  end

  def test_ensure_created
    schema = Droonga::WatchSchema.new(@context)
    schema.ensure_created

    dumped_commands = nil
    @context.open_database(@database_path.to_s) do |database|
      dumped_commands = Groonga::DatabaseDumper.dump(:context => @context,
                                                     :database => database)
    end
    assert_equal(<<-SCHEMA, dumped_commands)
table_create Keyword TABLE_PAT_KEY ShortText --normalizer NormalizerAuto

table_create Query TABLE_HASH_KEY ShortText

table_create Route TABLE_HASH_KEY ShortText

table_create Subscriber TABLE_HASH_KEY ShortText
column_create Subscriber last_modified COLUMN_SCALAR Time

column_create Query keywords COLUMN_VECTOR Keyword

column_create Subscriber route COLUMN_SCALAR Route
column_create Subscriber subscriptions COLUMN_VECTOR Query

column_create Keyword queries COLUMN_INDEX Query keywords

column_create Query subscribers COLUMN_INDEX Subscriber subscriptions
SCHEMA
  end
end
