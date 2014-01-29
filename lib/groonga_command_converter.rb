# -*- coding: utf-8 -*-
#
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

require "groonga/command/parser"
require "digest/sha1"
require "time"

module Droonga
  class GroongaCommandConverter
    STATUS_OK = 200

    def initialize(options={})
      @options = options
      @count = 0

      @command_parser = Groonga::Command::Parser.new
    end

    def convert(input, &block)
      @command_parser.on_command do |command|
        case command.name
        when "table_create"
          yield create_table_create_command(command)
        when "table_remove"
          yield create_table_remove_command(command)
        when "column_create"
          yield create_column_create_command(command)
        when "select"
          yield create_select_command(command)
        end
      end

      parsed_values = nil
      parsed_columns = nil
      @command_parser.on_load_start do |command|
        parsed_values = []
        parsed_columns = nil
      end
      @command_parser.on_load_columns do |command, columns|
        parsed_columns = columns
      end
      @command_parser.on_load_value do |command, value|
        parsed_values << value
      end
      @command_parser.on_load_complete do |command|
        command[:columns] = parsed_columns.join(",")
        command[:values] = parsed_values.to_json
        split_load_command_to_add_commands(command, &block)
      end

      input.each_line do |line|
        @command_parser << line
      end
      @command_parser.finish
    end

    private
    def create_message(type, body)
      id = @options[:id]
      if id.nil?
        id = new_unique_id
      else
        id = "#{id}:#{@count}"
        @count += 1
      end

      {
        :id => id,
        :date => format_date(@options[:date] || Time.now),
        :replyTo => @options[:reply_to],
        :statusCode => @options[:status_code] || STATUS_OK,
        :dataset => @options[:dataset],
        :type => type,
        :body => body,
      }
    end

    def new_unique_id
      now = Time.now
      now_msec = now.to_i * 1000 + now.usec
      random_string = rand(36 ** 16).to_s(36) # Base36
      Digest::SHA1.hexdigest("#{now_msec}:#{random_string}")
    end

    def format_date(time)
      time.iso8601
    end

    def create_table_create_command(command)
      create_message("table_create", command.arguments)
    end

    def create_table_remove_command(command)
      create_message("table_remove", command.arguments)
    end

    def create_column_create_command(command)
      create_message("column_create", command.arguments)
    end

    def split_load_command_to_add_commands(command, &block)
      columns = command[:columns].split(",")
      values = command[:values]
      values = JSON.parse(values)
      values.each do |record|
        body = {
          :table => command[:table],
        }

        record_values = {}
        record.each_with_index do |value, column_index|
          column = columns[column_index]
          if column == "_key"
            body[:key] = value
          else
            record_values[column.to_sym] = value
          end
        end
        body[:values] = record_values unless record_values.empty?

        yield create_message("add", body)
      end
    end

    def create_select_command(command)
      create_message("select", command.arguments)
    end
  end
end
