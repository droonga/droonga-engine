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

require "fluent/plugin/out_droonga"

module OutputStub
  class Worker
    def initialize(response)
      @response = response
    end

    def process_message(record)
      @response
    end

    def shutdown
    end
  end

  class Output < Fluent::DroongaOutput
    def initialize(response)
      @response = response
      super()
    end

    def create_worker
      Worker.new(@response)
    end
  end
end

class OutputTest < Test::Unit::TestCase
  setup
  def setup_fluent
    Fluent::Test.setup
  end

  def test_emit
    response = {}
    driver = create_driver("droonga.message", response)
    time = Time.parse("2012-10-26T08:45:42Z").to_i
    driver.run do
      driver.emit({"replyTo" => "127.0.0.1:2929/droonga.message"}, time)
    end
  end

  private
  def create_driver(tag, response)
    output = OutputStub::Output.new(response)
    driver = Fluent::Test::OutputTestDriver.new(output, tag)
    driver.configure(configuration)
    driver
  end

  def configuration
    <<-EOC
EOC
  end
end
