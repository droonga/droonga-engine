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
  def command(method_name, options={})
    Droonga::Command.new(method_name, options)
  end

  class ResolvePathTest < self
    def command
      super(:method_name)
    end

    def resolve_path(path, message)
      command.send(:resolve_path, path, message)
    end

    def test_nonexistent
      assert_equal(Droonga::Command::NONEXISTENT_PATH,
                   resolve_path("nonexistent.path", {}))
    end

    def test_top_level
      assert_equal("select",
                   resolve_path("type",
                                {
                                  "type" => "select"
                                }))
    end

    def test_nested
      assert_equal(10,
                   resolve_path("body.output.limit",
                                {
                                  "body" => {
                                    "output" => {
                                      "limit" => 10,
                                    },
                                  },
                                }))
    end
  end

  class MatchTest < self
    def command(patterns)
      super(:method_name, :patterns => patterns)
    end

    def match?(patterns, message)
      command(patterns).match?(message)
    end

    class EqualTest < self
      def test_same_value
        assert_true(match?([["type", :equal, "select"]],
                           {
                             "type" => "select"
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
