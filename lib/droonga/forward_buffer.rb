# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2015 Droonga Project
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
require "time"

require "droonga/loggable"
require "droonga/path"
require "droonga/safe_file_writer"
require "droonga/serf"

module Droonga
  class ForwardBuffer
    include Loggable

    SUFFIX = ".msgpack"

    attr_writer :on_forward

    def initialize(node_name)
      @on_forward = nil

      @packer = MessagePack::Packer.new
      @unpacker = MessagePack::Unpacker.new

      @target = node_name
      @serf = Serf.new(ENV["DROONGA_ENGINE_NAME"])

      dirname = node_name.gsub("/", ":")
      @data_directory = Path.intentional_buffer + dirname
      FileUtils.mkdir_p(@data_directory.to_s)
    end

    def add(message, destination)
      logger.trace("add: start")
      @serf.set_have_unprocessed_messages_for(@target)
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
      n_forwarded_messages = 0
      Pathname.glob("#{@data_directory}/*#{SUFFIX}").collect do |buffered_message_path|
        forwarded = forward(buffered_message_path)
        n_forwarded_messages += 1 if forwarded
      end
      if n_forwarded_messages > 0 and
           @process_messages_newer_than_timestamp
        logger.info("#{n_forwarded_messages} new messages forwarded. " +
                      "The boundary is now cleared.")
        @process_messages_newer_than_timestamp = nil
      end
      @serf.reset_have_unprocessed_messages_for(@target)
      logger.trace("start_forward: done")
    end

    def empty?
      @data_directory.children.empty?
    end

    def process_messages_newer_than(timestamp)
      @process_messages_newer_than_timestamp = timestamp
    end

    private
    def forward(buffered_message_path)
      logger.trace("forward: start (#{buffered_message_path})")
      file_contents = buffered_message_path.read
      @unpacker.feed(file_contents)
      buffered_message = @unpacker.read
      @unpacker.reset

      message     = buffered_message["message"]
      destination = buffered_message["destination"]

      forwarded = false

      if @process_messages_newer_than_timestamp
        message_timestamp = Time.parse(message["date"])
        logger.trace("Checking boundary of obsolete message",
                     :newer_than => @process_messages_newer_than_timestamp,
                     :message_at => message_timestamp)
        if @process_messages_newer_than_timestamp >= message_timestamp
          buffered_message = nil
        else
          logger.info("New message is detected.")
          # Don't clear the boundary for now, because older messages
          # forwarded by the dispatcher can be still buffered.
        end
      end

      if buffered_message
        logger.trace("forward: Forwarding buffered message",
                     :message     => message,
                     :destination => destination)
        message["xSender"] = "forward-buffer"
        on_forward(message, destination)
        forwarded = true
      end

      FileUtils.rm_f(buffered_message_path.to_s)
      logger.trace("forward: done (#{buffered_message_path})")

      forwarded
    end

    def file_path(time_stamp=Time.now)
      @data_directory + "#{time_stamp.iso8601(6)}#{SUFFIX}"
    end

    def on_forward(message, destination)
      @on_forward.call(message, destination) if @on_forward
    end

    def log_tag
      "[#{Process.ppid}] forward-buffer"
    end
  end
end
