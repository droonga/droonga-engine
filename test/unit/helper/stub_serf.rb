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

class StubSerf
  def initialize
    @have_unprocessed_messages_for = {}
  end

  def set_have_unprocessed_messages_for(target)
    @have_unprocessed_messages_for[target] = true
  end

  def reset_have_unprocessed_messages_for(target)
    @have_unprocessed_messages_for.delete(target)
  end
end
