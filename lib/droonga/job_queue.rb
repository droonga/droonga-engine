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

require "droonga/job_queue_schema"
require "msgpack"

module Droonga
  class JobQueue
    class << self
      def ensure_schema(database_path, queue_name)
        schema = JobQueueSchema.new(database_path, queue_name)
        schema.ensure_created
      end

      def open(database_path, queue_name)
        job_queue = new(database_path, queue_name)
        job_queue.open
        job_queue
      end
    end

    def initialize(database_path, queue_name)
      @database_path = database_path
      @queue_name = queue_name
    end

    def open
      @context = Groonga::Context.new
      @database = @context.open_database(@database_path)
      @context.encoding = :none

      @queue = @context[@queue_name]
    end

    def push_message(message)
      $log.trace("#{log_tag}: push_message: start")
      packed_message = message.to_msgpack
      @queue.push do |record|
        record.message = packed_message
      end
      $log.trace("#{log_tag}: push_message: done")
    end

    def pull_message
      packed_message = nil
      @queue.pull do |record|
        if record
          packed_message = record.message
          record.delete
        end
      end
      return nil unless packed_message
      MessagePack.unpack(packed_message)
    end

    def close
      @queue = nil
      if @database
        @database.close
        @context.close
        @database = @context = nil
      end
    end

    def log_tag
      "[#{Process.ppid}][#{Process.pid}] job_queue"
    end
  end
end
