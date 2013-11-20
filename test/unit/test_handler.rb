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

require "droonga/handler"

class HandlerTest < Test::Unit::TestCase
  class HandlableTest < self
    class SearchHandler < Droonga::Handler
      command :search
      def search(request)
        :search_response
      end
    end

    class StatusHandler < Droonga::Handler
      command :status
      def status(request)
        :status_response
      end
    end

    class Worker
      def context
        nil
      end
    end

    def setup
      context = Worker.new
      @search_handler = SearchHandler.new(context)
    end

    def test_true
      assert_true(@search_handler.handlable?(:search))
    end

    def test_false
      assert_false(@search_handler.handlable?(:status))
    end
  end
end
