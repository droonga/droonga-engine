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

class SystemStatusHandlerTest < Test::Unit::TestCase
  def setup
    setup_handler
  end

  def teardown
    teardown_handler
  end

  private
  def setup_handler
    @worker = StubWorker.new
    @messenger = Droonga::Test::StubHandlerMessenger.new
    @messenger.cluster = StubCluster.new
    @messenger.engine_state = StubEngineState.new
    @loop = nil
    @handler = Droonga::Plugins::System::StatusHandler.new(:name      => "name",
                                                           :context   => @worker.context,
                                                           :messenger => @messenger,
                                                           :loop      => @loop)
  end

  def teardown_handler
    @handler = nil
  end

  def process(request)
    message = Droonga::Test::StubHandlerMessage.new(request)
    @handler.handle(message)
  end

  class StubCluster
    def engine_nodes_status
      {
        "127.0.0.1:10031/droonga" => {
          "status" => "active",
        },
        "127.0.0.1:10032/droonga" => {
          "status" => "inactive",
        },
        "127.0.0.1:10033/droonga" => {
          "status" => "dead",
        },
      }
    end
  end

  class StubEngineState
    def name
      "127.0.0.1:10031/droonga"
    end

    def internal_name
      "127.0.0.1:12345/droonga"
    end
  end

  public
  def test_request
    request = {}
    response = process(request)
    status = {
      "nodes" => {
        "127.0.0.1:10031/droonga" => {
          "status" => "active",
        },
        "127.0.0.1:10032/droonga" => {
          "status" => "inactive",
        },
        "127.0.0.1:10033/droonga" => {
          "status" => "dead",
        },
      },
      "reporter" => "127.0.0.1:12345/droonga @ 127.0.0.1:10031/droonga",
    }
    assert_equal(status, response)
  end
end
