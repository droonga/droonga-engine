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
    # @option options [Array<Array>] :patterns The patterns to be matched
    #    against message. If all of the patterns are matched to a message,
    #    the command will be applied.
    def initialize(method_name, options)
      @method_name = method_name
      @options = options
    end

    def match?(message)
      patterns.all? do |pattern|
        match_pattern?(pattern, message)
      end
    end

    private
    def patterns
      @options[:patterns] || []
    end

    def match_pattern?(pattern, message)
      path, operator, *arguments = pattern
      target = path.split(".").inject(message) do |result, component|
        result[component]
      end
      apply_operator(operator, target, arguments)
    end

    def apply_operator(operator, target, arguments)
      case operator
      when :equal
        [target] == arguments
      else
        raise InvalidArgument, "Unknown operator"
      end
    end
  end
end
