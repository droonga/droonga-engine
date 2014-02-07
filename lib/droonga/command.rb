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

module Droonga
  class Command
    attr_reader :method_name
    # @option options [Array] :pattern The pattern to be matched
    #    against message. If the pattern is matched to a message,
    #    the command will be applied.
    #
    # @see MessageMatcher
    def initialize(method_name, options)
      @method_name = method_name
      @options = options
      @matcher = MessageMatcher.new(@options[:pattern])
    end

    def match?(message)
      @matcher.match?(message)
    end
  end
end
