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

require "fileutils"

require "groonga"

module Droonga
  class JobQueueSchema
    def initialize(database_path, queue_name)
      @database_path = database_path
      @queue_name = queue_name
    end

    def ensure_created
      ensure_database
      ensure_queue
    end

    private
    def ensure_database
      return if File.exist?(@database_path)
      FileUtils.mkdir_p(File.dirname(@database_path))
      create_context do |context|
        context.create_database(@database_path) do
        end
      end
    end

    def ensure_queue
      create_context do |context|
        context.open_database(@database_path) do
          Groonga::Schema.define(:context => context) do |schema|
            schema.create_table(@queue_name, :type => :array) do |table|
              table.text("request")
            end
          end
        end
      end
    end

    def create_context
      context = Groonga::Context.new
      begin
        yield(context)
      ensure
        context.close
      end
    end
  end
end
