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

      def header(return_code, error_message="")
        elapsed_time = Time.now.to_f - @start_time
        header = [return_code, @start_time, elapsed_time]
        header.push(error_message) unless error_message.empty?
        header
      end

      def execute(request)
        @start_time = Time.now.to_f

        command_class = Groonga::Command.find("table_create")
        @command = command_class.new("table_create", request)

        name = @command["name"]
        return [header(Status::INVALID_ARGUMENT, "Should not create anonymous table"), false] unless name

        options = parse_command
        Groonga::Schema.define(:context => @context) do |schema|
          schema.create_table(name, options)
        end
        [header(Status::SUCCESS), true]
      end

      private
      def parse_command
        options = {}
        parse_flags(options)
        parse_key_type(options)
        parse_value_type(options)
        parse_default_tokenizer(options)
        parse_normalizer(options)
        options
      end

      def parse_flags(options)
        options[:type] = :hash
        if @command.table_no_key?
          options[:type] = :array
        elsif @command.table_hash_key?
          options[:type] = :hash
        elsif @command.table_pat_key?
          options[:type] = :patricia_trie
        elsif @command.table_dat_key?
          options[:type] = :double_array_trie
        end
        if @command.key_with_sis? and @command.table_pat_key?
          options[:key_with_sis] = true
        end
        options
      end

      def parse_key_type(options)
        options[:key_type] = @command["key_type"] if @command["key_type"]
        options
      end

      def parse_value_type(options)
        options[:value_type] = @command["value_type"] if @command["value_type"]
        options
      end

      def parse_default_tokenizer(options)
        options[:default_tokenizer] = @command["default_tokenizer"] if @command["default_tokenizer"]
        options
      end

      def parse_normalizer(options)
        options[:normalizer] = @command["normalizer"] if @command["normalizer"]
        options
      end
    end
  end
end
