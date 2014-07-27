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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

module PluginHelper
  def setup_plugin(plugin_class)
    @worker = StubWorker.new
    @plugin = plugin_class.new(@worker)

    @messages = []
    stub(@plugin).emit do |*message|
      @messages << message
    end

    @posted = []
    stub(@plugin).post do |*message|
      @posted << message
    end
  end

  def teardown_plugin
    @plugin = nil
    @worker = nil
    @messages = nil
    @posted = nil
  end
end
