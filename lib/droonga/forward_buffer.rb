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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "fileutils"
require "json"
require "pathname"

require "droonga/loggable"
require "droonga/path"
require "droonga/safe_file_writer"

module Droonga
  class ForwardBuffer
    include Loggable

    SUFFIX = ".json"

    def initialize(node_name, params)
      @node_name = node_name
      @forwarder = params[:forwarder]

      @data_directory = Path.intentional_buffer + "#{@node_name}"
      FileUtils.mkdir_p(@data_directory.to_s)
    end

    def add(receiver, message, command, arguments, options)
      logger.trace("add: start")
      buffered_message = {
        "receiver"  => receiver,
        "message"   => message,
        "command"   => command,
        "arguments" => arguments,
        "options"   => options,
      }
      SafeFileWriter.write(file_path) do |output, file|
        output.puts(JSON.generate(buffered_message))
      end
      logger.trace("add: done")
    end

    def resume
      logger.trace("resume: start")
      Pathname.glob("#{@data_directory}/*#{SUFFIX}").collect do |buffered_message_path|
        output(buffered_message_path)
      end
      logger.trace("resume: done")
    end

    def empty?
      @data_directory.children.empty?
    end

    private
    def output(buffered_message_path)
      file_contents = Pathname(buffered_message_path).read
      buffered_message = JSON.parse(file_contents)
      @forwarder.output(buffered_message["receiver"],
                        buffered_message["message"],
                        buffered_message["command"],
                        buffered_message["arguments"],
                        buffered_message["options"])
      FileUtils.rm_f(buffered_message_path.to_s)
    end

    def file_path(timestamp=Time.now)
      @data_directory + "#{time_stamp.iso8601(6)}.#{SUFFIX}"
    end

    def log_tag
      "[#{Process.ppid}] forward-buffer"
    end
  end
end
