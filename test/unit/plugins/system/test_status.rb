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
    @messenger.engine_state = StubEngineState.new
    @loop = nil
    @handler = Droonga::Plugins::System::StatusHandler.new("name",
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

  class StubEngineState
    def all_nodes
      [
        "127.0.0.1:10031/droonga",
        "127.0.0.1:10032/droonga",
      ]
    end

    def live_nodes
      [
        "127.0.0.1:10031/droonga",
      ]
    end
  end

  public
  def test_request
    request = {}
    response = process(request)
    status = {
      "nodes" => {
        "127.0.0.1:10031/droonga" => {
          "live" => true,
        },
        "127.0.0.1:10032/droonga" => {
          "live" => false,
        },
      },
    }
    assert_equal(status, response)
  end
end
