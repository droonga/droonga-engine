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

require "droonga/message_matcher"

class MessageMatcherTest < Test::Unit::TestCase
  def matcher(pattern)
    Droonga::MessageMatcher.new(pattern)
  end

  class ResolvePathTest < self
    def resolve_path(path, message)
      matcher(nil).send(:resolve_path, path, message)
    end

    def test_nonexistent
      assert_equal(Droonga::MessageMatcher::NONEXISTENT_PATH,
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
    def match?(pattern, message)
      matcher(pattern).match?(message)
    end

    class EqualTest < self
      def test_same_value
        assert_true(match?(["type", :equal, "select"],
                           {
                             "type" => "select"
                           }))
      end

      def test_different_value
        assert_false(match?(["type", :equal, "select"],
                            {
                              "type" => "search",
                            }))
      end
    end

    class InTest < self
      def test_exist
        assert_true(match?(["type", :in, ["table_create", "table_remove"]],
                           {
                             "type" => "table_remove"
                           }))
      end

      def test_not_exist
        assert_false(match?(["type", :in, ["table_create", "table_remove"]],
                            {
                              "type" => "column_create",
                            }))
      end
    end

    class IncludeTest < self
      def test_exist
        assert_true(match?(["originalTypes", :include?, "select"],
                           {
                             "originalTypes" => ["search", "select"],
                           }))
      end

      def test_not_exist
        assert_false(match?(["originalTypes", :include?, "select"],
                            {
                              "originalTypes" => ["load"],
                            }))
      end

      def test_no_key
        assert_false(match?(["originalTypes", :include?, "select"],
                            {}))
      end

      def test_not_enumerable
        assert_false(match?(["originalTypes", :include?, "select"],
                            {
                              "originalTypes" => 29,
                            }))
      end
    end

    class ExistTest < self
      def test_exist
        assert_true(match?(["body.result", :exist?],
                           {
                             "body" => {
                               "result" => nil,
                             },
                           }))
      end

      def test_not_exist
        assert_false(match?(["body.result", :exist?],
                            {
                              "body" => nil,
                            }))
      end
    end
  end
end
