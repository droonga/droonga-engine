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
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

require "droonga/plugin"

module Droonga
  module Plugins
    module Groonga
      extend Plugin
      register("groonga")
    end
  end
end

require "droonga/plugins/groonga/generic_response"
require "droonga/plugins/groonga/select"
require "droonga/plugins/groonga/table_create"
require "droonga/plugins/groonga/table_remove"
require "droonga/plugins/groonga/column_create"

