# Copyright (C) 2015 Droonga Project
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "groonga"

module Droonga
  module DatabaseScanner
    def n_tables
      n_tables  = 0
      each_table do |table|
        n_tables += 1
      end
      n_tables
    end

    def n_columns
      n_columns = 0
      each_table do |table|
        n_columns += table.columns.size
      end
      n_columns
    end

    def n_records
      n_records = 0
      each_table do |table|
        unless index_only_table?(table)
          n_records += table.size
        end
      end
      n_records
    end

    def total_n_objects
      n_tables  = 0
      n_columns = 0
      n_records = 0
      each_table do |table|
        n_tables += 1
        n_columns += table.columns.size
        unless index_only_table?(table)
          n_records += table.size
        end
      end
      n_tables + n_columns + n_records
    end

    def each_table(&block)
      options = {
        :ignore_missing_object => true,
        :order_by => :key,
      }
      reference_tables = []
      @context.database.each(options) do |object|
        next unless table?(object)
        if reference_table?(object)
          reference_tables << object
          next
        end
        yield(object)
      end
      reference_tables.each do |reference_table|
        yield(object)
      end
    end

    def table?(object)
      object.is_a?(::Groonga::Table)
    end

    def reference_table?(table)
      table.support_key? and table?(table.domain)
    end

    def index_only_table?(table)
      return false if table.columns.empty?
      table.columns.all? do |column|
        index_column?(column)
      end
    end

    def index_column?(column)
      column.is_a?(::Groonga::IndexColumn)
    end

    def each_index_columns(&block)
      each_table do |table|
        table.columns.each do |column|
          yield(column) if index_column?(column)
        end
      end
    end
  end
end
