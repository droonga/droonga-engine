# Copyright (C) 2013 droonga project
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

require "droonga/job_queue_schema"

class JobQueueSchemaTest < Test::Unit::TestCase
  def setup
    @database_path = @temporary_directory + "droonga/db"
    @queue_name = "DroongaQueue"
  end

  def test_ensure_created
    schema = Droonga::JobQueueSchema.new(@database_path.to_s, @queue_name)

    assert_not_predicate(@database_path, :exist?)
    schema.ensure_created
    assert_predicate(@database_path, :exist?)

    context = Groonga::Context.new
    dumped_commands = nil
    context.open_database(@database_path.to_s) do |database|
      dumped_commands = Groonga::DatabaseDumper.dump(:context => context,
                                                     :database => database)
    end
    context.close
    assert_equal(<<-SCHEMA, dumped_commands)
table_create #{@queue_name} TABLE_NO_KEY
column_create #{@queue_name} value COLUMN_SCALAR Text
SCHEMA
  end
end
