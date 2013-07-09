# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 droonga project
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

require "groonga"

require "droonga/handler"

module Droonga
  class GroongaHandler < Droonga::Handler
    Droonga::HandlerPlugin.register("groonga", self)

    command :table_create
    def table_create(request)
      name = request["name"]
      Groonga::Schema.define(:context => @context) do |schema|
        schema.create_table(name)
      end
      outputs = [true]
      post(outputs)
    end
  end
end
