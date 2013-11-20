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

require "droonga/adapter"

class AdapterTest < Test::Unit::TestCase
  class AdaptTest < self
    class GroongaAdapter < Droonga::Adapter
      command :select
      def select(request)
        post(:search) do |response|
          # do nothing
        end
        :selected
      end
    end

    def setup
      @worker = StubWorker.new
      @groonga_adapter = GroongaAdapter.new(@worker)
    end

    def test_called
      request = nil
      assert_equal(:selected, @groonga_adapter.adapt(:select, request))
    end

    def test_post
      request = nil
      response = nil
      mock(@dispatcher).post(:search).yields(response)
      @groonga_adapter.adapt(:select, request)
    end
  end
end
