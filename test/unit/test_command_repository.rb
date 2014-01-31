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

require "droonga/command_repository"

class CommandRepositoryTest < Test::Unit::TestCase
  def setup
    @repository = Droonga::CommandRepository.new
  end

  class FindTest < self
    def setup
      super
      @command = Droonga::Command.new(:select,
                                      :patterns => [["type", :equal, "select"]])
      @repository.register(@command)
    end

    def test_match
      assert_equal(@command, @repository.find({ "type" => "select" }))
    end

    def test_not_match
      assert_nil(@repository.find({ "type" => "search" }))
    end
  end
end
