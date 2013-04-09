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

require "droonga/plugin/handler_search"

class SearchHandlerTest < Test::Unit::TestCase
  def setup
    setup_database
    setup_handler
  end

  def teardown
    teardown_handler
    teardown_database
  end

  private
  def setup_database
    restore(fixture_data("document.grn"))
    @database = Groonga::Database.open(@database_path.to_s)
  end

  def teardown_database
    @database.close
    @database = nil
  end

  def setup_handler
    @handler = Droonga::SearchHandler.new(Groonga::Context.default)
  end

  def teardown_handler
    @handler = nil
  end

  class NoParameterTest < self
    def test_empty
      assert_equal({}, @handler.search({}))
    end
  end
end
