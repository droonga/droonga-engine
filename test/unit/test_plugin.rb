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

require "droonga/plugin"

class PluginTest < Test::Unit::TestCase
  class PluggableTest < self
    class SearchPlugin < Droonga::Plugin
      command :search
      def search(request)
        :search_response
      end
    end

    class Worker
      def context
        nil
      end
    end

    def setup
      context = Worker.new
      @search_plugin = SearchPlugin.new(context)
    end

    def test_true
      assert_true(@search_plugin.processable?(:search))
    end

    def test_false
      assert_false(@search_plugin.processable?(:status))
    end
  end
end
