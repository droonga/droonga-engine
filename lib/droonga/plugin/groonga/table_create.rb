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
require "groonga/command/table-create"

module Droonga
  class GroongaHandler
    class TableCreate
      def initialize(context)
        @context = context
      end

      def execute(request)
        command_class = Groonga::Command.find("table_create")
        command = command_class.new("table_create", request)

        name = command["name"]
        return [false] unless name

        options = parse_command(command)
        Groonga::Schema.define(:context => @context) do |schema|
          schema.create_table(name, options)
        end
        [true]
      end

      private
      def parse_command(command)
        options = {}
        parse_flags(options, command)
        parse_key_type(options, command)
        parse_value_type(options, command)
        parse_default_tokenizer(options, command)
        parse_normalizer(options, command)
        options
      end

      def parse_flags(options, command)
        options[:type] = :hash
        if command.table_no_key?
          options[:type] = :array
        elsif command.table_hash_key?
          options[:type] = :hash
        elsif command.table_pat_key?
          options[:type] = :patricia_trie
        elsif command.table_dat_key?
          options[:type] = :double_array_trie
        end
        if command.key_with_sis? and command.table_pat_key?
          options[:key_with_sis] = true
        end
        options
      end

      def parse_key_type(options, command)
        options[:key_type] = command["key_type"] if command["key_type"]
        options
      end

      def parse_value_type(options, command)
        options[:value_type] = command["value_type"] if command["value_type"]
        options
      end

      def parse_default_tokenizer(options, command)
        options[:default_tokenizer] = command["default_tokenizer"] if command["default_tokenizer"]
        options
      end

      def parse_normalizer(options, command)
        options[:normalizer] = command["normalizer"] if command["normalizer"]
        options
      end
    end
  end
end
