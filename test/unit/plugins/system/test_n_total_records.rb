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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "droonga/plugins/system"

class SystemNTotalRecordsHandlerTest < Test::Unit::TestCase
  def setup
    setup_database
    setup_handler
    setup_tables
    setup_records
  end

  def teardown
    teardown_database
    teardown_handler
  end

  private
  def setup_handler
    @worker = StubWorker.new
    @messenger = Droonga::Test::StubHandlerMessenger.new
    @loop = nil
    @handler = Droonga::Plugins::System::NTotalRecordsHandler.new("name",
                                                                  @worker.context,
                                                                  @messenger,
                                                                  @loop)
  end

  def teardown_handler
    @handler = nil
  end

  def process(request)
    message = Droonga::Test::StubHandlerMessage.new(request)
    @handler.handle(message)
  end

  def setup_tables
    Groonga::Schema.define do |schema|
      schema.create_table("Users",
                          :type => :hash,
                          :key_type => :short_text)
      schema.create_table("Groups",
                          :type => :hash,
                          :key_type => :short_text)
    end
  end

  def setup_records
    @worker.context["Users"].add("Alice")
    @worker.context["Users"].add("Bob")
    @worker.context["Groups"].add("Users")
    @worker.context["Groups"].add("Administrators")
  end

  public
  def test_request
    request = {}
    response = process(request)
    n_total_records = 4
    assert_equal(n_total_records, response)
  end
end
