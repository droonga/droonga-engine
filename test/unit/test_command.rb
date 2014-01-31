# Copyright (C) 2014 Droonga Project
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

require "droonga/command"

class CommandTest < Test::Unit::TestCase
  class MatchTest < self
    def command(patterns)
      Droonga::Command.new(:method_name, :patterns => patterns)
    end

    def match?(patterns, message)
      command(patterns).match?(message)
    end

    class EqualTest < self
      def test_top_level
        assert_true(match?([["type", :equal, "select"]],
                           {
                             "type" => "select"
                           }))
      end

      def test_nested
        assert_true(match?([["body.output.limit", :equal, 10]],
                           {
                             "body" => {
                               "output" => {
                                 "limit" => 10,
                               },
                             },
                           }))
      end

      def test_different_value
        assert_false(match?([["type", :equal, "select"]],
                            {
                              "type" => "search",
                            }))
      end
    end
  end
end
