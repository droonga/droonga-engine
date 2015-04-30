# Copyright (C) 2015 Droonga Project
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

class SystemStatisticsObjectCountPerVolumeHandlerTest < Test::Unit::TestCase
  def setup
    setup_database
    setup_handler

    Groonga::Schema.define do |schema|
      schema.create_table("Books", :type => :hash)
      schema.change_table("Books") do |table|
        table.column("title", "ShortText", :type => :scalar)
      end
    end
    Groonga::Context.default["Books"].add("sample")
  end

  def teardown
    teardown_handler
    teardown_database
  end

  private
  def setup_handler
    @worker = StubWorker.new
    @messenger = Droonga::Test::StubHandlerMessenger.new
    @loop = nil
    handler_params = {
      :name      => "name",
      :label     => "label",
      :context   => @worker.context,
      :messenger => @messenger,
      :loop      => @loop,
    }
    @handler = Droonga::Plugins::System::StatisticsObjectCountPerVolumeHandler.new(handler_params)
  end

  def teardown_handler
    @handler = nil
  end

  def process(request)
    message = Droonga::Test::StubHandlerMessage.new(request)
    @handler.handle(message)
  end

  public
  data(
    :all => {
      :request => {
        "output" => [
          "tables",
          "columns",
          "records",
          "total",
        ],
      },
      :expected => {
        "label" => {
          "tables"  => 1,
          "columns" => 1,
          "records" => 1,
          "total"   => 3,
        },
      },
    },
    :tables => {
      :request => {
        "output" => [
          "tables",
        ],
      },
      :expected => {
        "label" => {
          "tables"  => 1,
        },
      },
    },
    :columns => {
      :request => {
        "output" => [
          "columns",
        ],
      },
      :expected => {
        "label" => {
          "columns" => 1,
        },
      },
    },
    :records => {
      :request => {
        "output" => [
          "records",
        ],
      },
      :expected => {
        "label" => {
          "records" => 1,
        },
      },
    },
    :total => {
      :request => {
        "output" => [
          "total",
        ],
      },
      :expected => {
        "label" => {
          "total"   => 3,
        },
      },
    },
    :nothing => {
      :request => {
        "output" => [],
      },
      :expected => {
        "label" => {},
      },
    },
    :no_parameter => {
      :request => {},
      :expected => {
        "label" => {},
      },
    },
  )
  def test_output(data)
    request = data[:request]
    response = process(request)
    assert_equal(data[:expected], response)
  end
end
