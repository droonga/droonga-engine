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

require "droonga/legacy_plugin"

class LegacyPluginTest < Test::Unit::TestCase
  class PluggableTest < self
    class DummyTypePlugin < Droonga::LegacyPlugin
      extend Droonga::PluginRegisterable
    end

    class DummyPlugin < DummyTypePlugin
      command :dummy
      def dummy(request)
        :dummy_response
      end
    end

    class UnknownPlugin < DummyTypePlugin
      command :unknown
      def unknown(request)
        :unknown_response
      end
    end

    def setup
      @dummy_plugin = DummyPlugin.new
    end

    def test_processable
      assert_true(@dummy_plugin.processable?(:dummy))
    end

    def test_not_processable
      assert_false(@dummy_plugin.processable?(:unknown))
    end
  end
end
