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

require "groonga/command"
require "digest/sha1"
require "time"

module Droonga
  class GroongaCommandConverter
    STATUS_OK = 200.freeze

    def initialize(options={})
      @options = options
      @count = 0
    end

    def convert(input, &block)
      @command = Groonga::Command::Parser.parse(input)
      case @command.name
      when "table_create"
        yield create_table_create_command
      when "column_create"
        yield create_column_create_command
      when "load"
        split_load_command_to_add_commands(&block)
      when "select"
        yield create_select_command
      end
    end

    private
    def create_envelope(type, body)
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

    def create_table_create_command
      create_envelope("table_create", @command.to_hash)
    end

    def create_column_create_command
      create_envelope("column_create", @command.to_hash)
    end

    def split_load_command_to_add_commands(&block)
      columns = @command[:columns].split(",")
      values = @command[:values]
      values = JSON.parse(values)
      values.each do |record|
        body = {
          :table => @command[:table],
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

        yield create_envelope("add", body)
      end
    end

    def create_select_command
      create_envelope("select", @command.to_hash)
    end
  end
end
