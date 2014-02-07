# Copyright (C) 2013 Droonga Project
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
  class HandlerMessage
    attr_reader :raw
    def initialize(raw)
      @raw = raw
    end

    def validate
      unless task.is_a?(Hash)
        raise "<task> value isn't object: <#{@raw.inspect}>"
      end

      unless step.is_a?(Hash)
        raise "<task/step> value isn't object: <#{@raw.inspect}>"
      end
    end

    def [](name)
      @raw[name]
    end

    def body
      @body ||= self["body"]
    end

    def task
      @task ||= body["task"]
    end

    def step
      @step ||= task["step"]
    end

    def request
      @request ||= step["body"]
    end

    def id
      @id ||= body["id"]
    end

    def descendants
      @descendants ||= body["descendants"]
    end
  end
end
