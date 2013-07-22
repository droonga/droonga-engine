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

require "droonga/command_mapper"

class CommandMapperTest < Test::Unit::TestCase
  class RegisterTest < self
    def setup
      @command_mapper = Droonga::CommandMapper.new
    end

    def test_name
      @command_mapper.register(:select)
      assert_equal(:select, @command_mapper[:select])
    end

    def test_different_method_name
      @command_mapper.register(:command_name => :method_name)
      assert_equal(:method_name, @command_mapper[:command_name])
    end

    def test_multiple_pairs
      map = {
        :command_name_1 => :command_name_1,
        :command_name_2 => :command_name_2,
      }
      @command_mapper.register(map)
      assert_equal(["command_name_1", "command_name_2"],
                   @command_mapper.commands)
    end
  end
end
