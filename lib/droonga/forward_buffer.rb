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
require "pathname"
require "msgpack"

require "droonga/loggable"
require "droonga/path"
require "droonga/safe_file_writer"

module Droonga
  class ForwardBuffer
    include Loggable

    SUFFIX = ".json"

    def initialize(node_name, forwarder)
      @forwarder = forwarder

      @packer = MessagePack::Packer.new
      @unpacker = MessagePack::Unpacker.new

      dirname = node_name.gsub("/", ":")
      @data_directory = Path.intentional_buffer + dirname
      FileUtils.mkdir_p(@data_directory.to_s)
    end

    def add(message, destination)
      logger.trace("add: start")
      buffered_message = {
        "message"     => message,
        "destination" => destination,
      }
      @packer.pack(buffered_message)
      SafeFileWriter.write(file_path) do |output, file|
        output.puts(@packer.to_s)
      end
      @packer.clear
      logger.trace("add: done")
    end

    def start_forward
      logger.trace("start_forward: start")
      Pathname.glob("#{@data_directory}/*#{SUFFIX}").collect do |buffered_message_path|
        output(buffered_message_path)
      end
      logger.trace("start_forward: done")
    end

    def empty?
      @data_directory.children.empty?
    end

    private
    def output(buffered_message_path)
      logger.trace("output: start (#{buffered_message_path})")
      time_stamp = buffered_message_path.basename(SUFFIX)
      file_contents = buffered_message_path.read
      @unpacker.feed(file_contents)
      buffered_message = @unpacker.read
      @unpacker.reset
      @forwarder.forward(buffered_message["message"],
                         buffered_message["destination"])
      FileUtils.rm_f(buffered_message_path.to_s)
      logger.trace("output: done (#{buffered_message_path})")
    end

    def file_path(time_stamp=Time.now)
      @data_directory + "#{time_stamp.iso8601(6)}#{SUFFIX}"
    end

    def log_tag
      "[#{Process.ppid}] forward-buffer"
    end
  end
end
