# Copyright (C) 2015 Droonga Project
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

require "fileutils"
require "droonga/path"

module Droonga
  class Restarter
    class << self
      def restart(wait_for_next=nil)
        new.restart(wait_for_next)
      end
    end

    def restart(wait_for_next=nil)
      FileUtils.touch(Path.restart.to_s)
      sleep(wait_for_next) if wait_for_next
    end
  end
end
