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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "droonga/test"

require_relative "helper/sandbox"
require_relative "helper/fixture"
require_relative "helper/stub_worker"
require_relative "helper/plugin_helper"
require_relative "helper/watch_helper"
require_relative "helper/distributed_search_planner_helper"

class Test::Unit::TestCase
  include ::Sandbox
  include ::Fixture
end
