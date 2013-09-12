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

module Droonga
  class GroongaCommandConverter
    STATUS_OK = 200.freeze

    def initialize
    end

    def convert(input, options={}, &block)
      @command = Groonga::Command::Parser.parse(input)
      @options = options

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
    def create_envelope(type, body, options={})
      {
        :id => options[:id] || new_unique_id,
        :date => options[:date] || current_date,
        :replyTo => options[:reply_to],
        :statusCode => options[:status_code] || STATUS_OK,
        :dataset => options[:dataset],
        :type => type,
        :body => body
      }
    end

    def new_unique_id
      nil
    end

    def current_date
      nil
    end

    def create_table_create_command
      body = {
        :name => @command[:name],
        :flags => @command[:flags],
        :key_type => @command[:key_type],
        :value_type => @command[:value_type],
        :default_tokenizer => @command[:default_tokenizer],
      }
      create_envelope("table_create", body)
    end

    def create_column_create_command
      body = {
        :table => @command[:table],
        :name => @command[:name],
        :flags => @command[:flags],
        :type => @command[:type],
        :source => @command[:source],
      }
      create_envelope("column_create", body)
    end

    def split_load_command_to_add_commands(&block)
      columns = @command[:columns].split(",")
      values = @command[:values]
      values = JSON.parse(values)
      values.each do |record|
        body = {
          :table => @command[:table],
        }

        record.each_with_index do |value, column_index|
          column = columns[column_index]
          if column == "_key"
            body[:key] = value
          else
            body[:values][column.to_sym] = value
          end
        end

        yield create_envelope("add", body)
      end
    end

    def create_select_command
      body = {
        :table => @command[:table],
        :sortby => @command[:sortby],
        :scorer => @command[:scorer],
        :query => @command[:query],
        :filter => @command[:filter],
        :conditions => @command[:conditions],
        :drilldown => @command[:drilldown],
        :output_columns => @command[:output_columns],
      }
      create_envelope("select", body)
    end
  end
end
