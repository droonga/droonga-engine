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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

module Droonga
  # It checks whether the pattern matches against a message.
  #
  # It provides the small language. Here is the pattern syntax.
  #
  #   * PATTERN = [TARGET_PATH, OPERATOR, ARGUMENT*]
  #   * PATTERN = [PATTERN, LOGICAL_OPERATOR, PATTERN]
  #   * TARGET_PATH = "COMPONENT(.COMPONENT)*"
  #   * OPERATOR = :equal, :in, :include, :exist, :start_with
  #                (More operators may be added in the future.
  #                 For example, :end_with and so on.)
  #   * ARGUMENT = OBJECT_DEFINED_IN_JSON
  #   * LOGICAL_OPERATOR = :or (:add will be added.)
  #
  # For example:
  #
  # ```
  # ["type", :equal, "search"]
  # ```
  #
  # matches to the following message:
  #
  # ```
  # {"type" => "search"}
  # ```
  #
  # Another example:
  #
  # ```
  # ["body.output.limit", :equal, 10]
  # ```
  #
  # matches to the following message:
  #
  # ```
  # {
  #   "body" => {
  #     "output" => {
  #       "limit" => 10,
  #     },
  #   },
  # }
  # ```
  class MessageMatcher
    # @param [Array] pattern The pattern to be matched against a message.
    def initialize(pattern)
      path, operator, *arguments = pattern
      @path_components = path.split(".")
      @operator = operator
      @arguments = arguments
    end

    def match?(message)
      target = extract_target(message)
      apply_operator(target)
    end

    private
    NONEXISTENT_PATH = Object.new
    def extract_target(message)
      result = message
      @path_components.each do |component|
        return NONEXISTENT_PATH unless result.is_a?(Hash)
        result = result[component]
      end
      result
    end

    def apply_operator(target)
      case @operator
      when :equal
        [target] == @arguments
      when :in
        @arguments.any? do |argument|
          argument.include?(target)
        end
      when :include
        return false unless target.respond_to?(:include?)
        @arguments.any? do |argument|
          target.include?(argument)
        end
      when :exist
        target != NONEXISTENT_PATH
      when :start_with
        return false unless target.respond_to?(:start_with?)
        @arguments.any? do |argument|
          target.start_with?(argument)
        end
      else
        raise ArgumentError, "Unknown operator: <#{@operator}>"
      end
    end
  end
end
