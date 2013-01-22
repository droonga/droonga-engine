# -*- coding: utf-8 -*-

require 'groonga'

module Fluent
  module Kotoumi
    class Session
      def initialize(database, queuename)
        Groonga::Database.open(database)
        @ctx = Groonga::Context.default
        @queuename = queuename
      end

      def process_message(tag, time, record)
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
end
