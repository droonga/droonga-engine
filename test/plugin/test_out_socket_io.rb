require 'helper'

class SocketIOOutputTest < Test::Unit::TestCase

  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    dest http://localhost:3000
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::OutputTestDriver.new(Fluent::SocketIOOutput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal "http://localhost:3000", d.instance.dest
  end

end
