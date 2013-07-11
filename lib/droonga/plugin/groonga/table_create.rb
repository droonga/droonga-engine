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

module Droonga
  class GroongaHandler
    class TableCreate
      def initialize(context)
        @context = context
      end

      def execute(request)
        name = request["name"]
        options = parse_request(request)
        Groonga::Schema.define(:context => @context) do |schema|
          schema.create_table(name, options)
        end
        [true]
      end

      private
      def parse_request(request)
        options = {}
        options.merge!(parse_flags(request))
        options.merge!(parse_key_type(request))
        options
      end

      def parse_flags(request)
        options = {:type => :hash}
        if request["flags"]
          request["flags"].split(/\|/).each do |flag|
            case flag
            when "TABLE_NO_KEY"
              options[:type] = :array
            when "TABLE_HASH_KEY"
              options[:type] = :hash
            when "TABLE_PAT_KEY"
              options[:type] = :patricia_trie
            when "TABLE_DAT_KEY"
              options[:type] = :double_array_trie
            when "KEY_WITH_SIS"
              options[:key_with_sis] = true
            end
          end
          if options[:key_with_sis]
            options[:key_with_sis] = false unless options[:type] == :patricia_trie
          end
        end
        options
      end

      def parse_key_type(request)
        options = {}
        options[:key_type] = request["key_type"] if request["key_type"]
        options
      end
    end
  end
end
