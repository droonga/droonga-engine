# Copyright (C) 2013-2014 Droonga Project
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
  class OutputMessage
    def initialize(raw_message)
      @raw_message = raw_message
    end

    def adapted_message
      # TODO: We can create adapted message non-destructively.
      # If it is not performance issue, it is better that we don't
      # change message destructively. Consider about it later.
      @raw_message
    end

    def status_code
      @raw_message["statusCode"]
    end

    def status_code=(status_code)
      @raw_message["statusCode"] = status_code
    end

    def errors
      @raw_message["errors"]
    end

    def errors=(errors)
      @raw_message["errors"] = errors
    end

    def body
      @raw_message["body"]
    end

    def body=(body)
      @raw_message["body"] = body
    end
  end
end
