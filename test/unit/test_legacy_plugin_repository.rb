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

require "droonga/legacy_plugin_repository"

class LegacyPluginRepositoryTest < Test::Unit::TestCase
  def setup
    @repository = Droonga::LegacyPluginRepository.new
  end

  class StubPlugin
    attr_reader :arguments
    def initialize(*arguments)
      @arguments = arguments
    end
  end

  def test_register
    @repository.register("stub", StubPlugin)
    assert_equal(StubPlugin, @repository["stub"])
  end

  def test_enumerable
    @repository.register("stub1", StubPlugin)
    @repository.register("stub2", StubPlugin)
    assert_equal([
                   ["stub1", StubPlugin],
                   ["stub2", StubPlugin],
                 ],
                 @repository.to_a)
  end

  sub_test_case("[]") do
    def setup
      super
      @repository.register("stub", StubPlugin)
    end

    def test_nonexistent
      assert_nil(@repository["nonexistent"])
    end

    def test_existent
      assert_equal(StubPlugin, @repository["stub"])
    end
  end

  sub_test_case("clear") do
    def setup
      super
      @repository.register("stub", StubPlugin)
    end

    def test_clear
      assert_equal([["stub", StubPlugin]], @repository.to_a)
      @repository.clear
      assert_equal([], @repository.to_a)
    end
  end

  sub_test_case("instantiate") do
    def setup
      super
      @repository.register("stub", StubPlugin)
    end

    def test_no_arguments
      plugin = @repository.instantiate("stub")
      assert_equal([], plugin.arguments)
    end

    def test_have_arguments
      plugin = @repository.instantiate("stub", "Hello", "World")
      assert_equal(["Hello", "World"], plugin.arguments)
    end
  end
end
