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

require "droonga/catalog/base"

module Droonga
  module Catalog
    class Version1 < Base
      def initialize(*args)
        super
        normalize_input_adapter
        normalize_output_adapter
      end

      private
      def normalize_input_adapter
        @data["input_adapter"] ||= {}
        @data["input_adapter"]["plugins"] ||= @options["plugins"]
      end

      def normalize_output_adapter
        @data["output_adapter"] ||= {}
        @data["output_adapter"]["plugins"] ||= @options["plugins"]
      end
    end
  end
end
