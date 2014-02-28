# Copyright (C) 2013-2014 Droonga Project
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

require "droonga/plugins/groonga"

class GroongaHandlerTest < Test::Unit::TestCase
  include PluginHelper

  def setup
    setup_database
    setup_plugin
  end

  def teardown
    teardown_plugin
    teardown_database
  end

  private
  def setup_plugin
    @handler = Droonga::Test::StubHandler.new
    @messenger = Droonga::Test::StubHandlerMessenger.new
  end

  def teardown_plugin
  end

  def dump
    database_dumper = Groonga::DatabaseDumper.new(:database => @database)
    database_dumper.dump
  end

  def process(command, request)
    message = Droonga::Test::StubHandlerMessage.new(request)
    handler = create_handler
    handler.handle(message)
  end

  NORMALIZED_START_TIME = Time.parse("2013-07-11T16:04:28+0900").to_i
  NORMALIZED_ELAPSED_TIME = 1
  def normalize_header(header)
    start_time = NORMALIZED_START_TIME
    elapsed_time = NORMALIZED_ELAPSED_TIME
    [header[0], start_time, elapsed_time]
  end

  NORMALIZED_HEADER_SUCCESS = [
    Droonga::Plugins::Groonga::Status::SUCCESS,
    NORMALIZED_START_TIME,
    NORMALIZED_ELAPSED_TIME,
  ]
  NORMALIZED_HEADER_INVALID_ARGUMENT = [
    Droonga::Plugins::Groonga::Status::INVALID_ARGUMENT,
    NORMALIZED_START_TIME,
    NORMALIZED_ELAPSED_TIME,
  ]
end
