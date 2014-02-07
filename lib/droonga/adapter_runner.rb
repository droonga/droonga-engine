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

require "droonga/input_adapter"
require "droonga/output_adapter"

module Droonga
  class AdapterRunner
    def initialize(dispatcher, input_adapter_options, output_adapter_options)
      @dispatcher = dispatcher
      @input_adapter = InputAdapter.new(self, input_adapter_options)
      @output_adapter = OutputAdapter.new(self, output_adapter_options)
    end

    def shutdown
      @input_adapter.shutdown
      @output_adapter.shutdown
    end

    def adapt_input(message)
      @input_adapter.adapt(message)
    end

    def adapt_output(message)
      @output_adapter.adapt(message)
    end
  end
end
