# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Kotoumi project
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

require 'groonga'

module Kotoumi
  class Worker
    def initialize(database, queue_name)
      Groonga::Database.open(database)
      @ctx = Groonga::Context.default
      @queuename = queue_name
    end

    def process_message(record)
      return {
        "main-search-result" => {
          startTime: "2001-08-02T10:45:23.5+09:00",
          elapsedTime: 123.456,
          count: 123,
          attributes: [
            { name: "name", type: "ShortText", vector: false },
            { name: "age", type: "UInt32", vector: false }
          ],
          records: [ ["a", 10], ["b", 20] ]
        }
      }
    end
  end
end
