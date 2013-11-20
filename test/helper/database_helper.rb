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

module DatabaseHelper
  def setup_database
    FileUtils.rm_rf(@database_path.dirname.to_s)
    FileUtils.mkdir_p(@database_path.dirname.to_s)
    @database = Groonga::Database.create(:path => @database_path.to_s)
  end

  def teardown_database
    @database.close
    @database = nil
    FileUtils.rm_rf(@database_path.dirname.to_s)
  end
end
