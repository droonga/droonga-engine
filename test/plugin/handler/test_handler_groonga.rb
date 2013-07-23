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

require "droonga/plugin/handler_groonga"

class GroongaHandlerTest < Test::Unit::TestCase
  def setup
    setup_database
    setup_handler
  end

  def teardown
    teardown_handler
    teardown_database
  end

  private
  def setup_database
    FileUtils.rm_rf(@database_path.dirname.to_s)
    FileUtils.mkdir_p(@database_path.dirname.to_s)
    @database = Groonga::Database.create(:path => @database_path.to_s)
  end

  def teardown_database
    @database.close
    @database = nil
    FileUtils.rm_rf(@database_path.dirname.to_s)
  end

  def setup_handler
    @worker = StubWorker.new
    @handler = Droonga::GroongaHandler.new(@worker)
  end

  def teardown_handler
    @handler = nil
  end

  private
  def dump
    database_dumper = Groonga::DatabaseDumper.new(:database => @database)
    database_dumper.dump
  end

  NORMALIZED_START_TIME = Time.parse("2013-07-11T16:04:28+0900").to_i
  NORMALIZED_ELAPSED_TIME = 1
  def normalize_header(header)
    start_time = NORMALIZED_START_TIME
    elapsed_time = NORMALIZED_ELAPSED_TIME
    [header[0], start_time, elapsed_time]
  end
end
