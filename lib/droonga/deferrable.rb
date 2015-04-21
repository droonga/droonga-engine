# Copyright (C) 2015 Droonga Project
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
  module Deferrable
    attr_writer :on_failure

    def wait_until_ready(target)
      target.on_ready = lambda do
        on_ready
      end
    end

    def on_ready=(callback)
      @on_ready_callbacks ||= []
      if callback
        @on_ready_callbacks << callback
      else
        @on_ready_callbacks.clear
      end
      callback
    end

    private
    def on_ready
      if @on_ready_callbacks
        @on_ready_callbacks.each do |callback|
          callback.call
        end
      end
    end

    def on_failure
      @on_failure.call if @on_failure
    end
  end
end
