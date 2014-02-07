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

module Droonga
  class Command
    attr_reader :method_name
    #
    #
    # @option options [Array] :pattern The pattern to be matched
    #    against message. If the pattern is matched to a message,
    #    the command will be applied.
    #
    #    Here is pattern syntax.
    #
    #      * PATTERN = [TARGET_PATH, OPERATOR, ARGUMENTS*]
    #      * PATTERN = [PATTERN, LOGICAL_OPERATOR, PATTERN]
    #      * TARGET_PATH = "COMPONENT(.COMPONENT)*"
    #      * OPERATOR = :equal, :in, :include?
    #                   (More operators may be added in the future.
    #                    For example, :exist?, :start_with and so on.)
    #      * ARGUMENTS = OBJECT_DEFINED_IN_JSON*
    #      * LOGICAL_OPERATOR = :or (:add will be added.)
    #
    #    For example:
    #
    #    ```
    #    ["type", :equal, "search"]
    #    ```
    #
    #    matches to the following message:
    #
    #    ```
    #    {"type" => "search"}
    #    ```
    #
    #    Another example:
    #
    #    ```
    #    ["body.output.limit", :equal, 10]
    #    ```
    #
    #    matches to the following message:
    #
    #    ```
    #    {
    #      "body" => {
    #        "output" => {
    #          "limit" => 10,
    #        },
    #      },
    #    }
    #    ```
    def initialize(method_name, options)
      @method_name = method_name
      @options = options
    end

    def match?(message)
      match_pattern?(@options[:pattern], message)
    end

    private
    def match_pattern?(pattern, message)
      return false if pattern.nil?
      path, operator, *arguments = pattern
      target = resolve_path(path, message)
      apply_operator(operator, target, arguments)
    end

    NONEXISTENT_PATH = Object.new
    def resolve_path(path, message)
      path.split(".").inject(message) do |result, component|
        return NONEXISTENT_PATH if result.nil?
        result[component]
      end
    end

    def apply_operator(operator, target, arguments)
      case operator
      when :equal
        [target] == arguments
      when :in
        arguments.any? do |argument|
          argument.include?(target)
        end
      when :include?
        return false unless target.respond_to?(:include?)
        arguments.any? do |argument|
          target.include?(argument)
        end
      else
        raise ArgumentError, "Unknown operator"
      end
    end
  end
end
