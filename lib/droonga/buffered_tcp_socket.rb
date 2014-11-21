# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Droonga Project
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

require "coolio"

require "droonga/loggable"

module Droonga
  class BufferedTCPSocket < Coolio::TCPSocket
    include Loggable

    def initialize(socket, data_directory)
      super(socket)
      @data_directory = data_directory
      @_write_buffer = []
    end

    def on_connect
      logger.trace("connected to #{@remote_host}:#{@remote_port}")
    end

    def write(data)
      chunk = Chunk.new(@data_directory, data, Time.now, 0)
      chunk.buffering
      @_write_buffer << chunk
      schedule_write
      data.bytesize
    end

    def on_writable
      until @_write_buffer.empty?
        chunk = @_write_buffer.shift
        begin
          written_size = @_io.write_nonblock(chunk.data)
          if written_size == chunk.data.bytesize
            chunk.written
          else
            chunk.written_partial(written_size)
            @_write_buffer.unshift(chunk)
            break
          end
        rescue Errno::EINTR
          @_write_buffer.unshift(chunk)
          return
        rescue SystemCallError, IOError, SocketError
          @_write_buffer.unshift(chunk)
          return close
        end
      end

      if @_write_buffer.empty?
        disable_write_watcher
        on_write_complete
      end
    end

    def resume
      @_write_buffer = (load_chunks + @_write_buffer).sort_by do |chunk|
        chunk.time_stamp
      end
    end

    private
    def load_chunks
      FileUtils.mkdir_p(@data_directory.to_s)
      chunk_loader = ChunkLoader.new(@data_directory)
      chunk_loader.load
    end

    def log_tag
      "[#{Process.ppid}] buffered-tcp-socket"
    end

    class ChunkLoader
      def initialize(path)
        @path = path
      end

      def have_any_chunk?
        @path.opendir do |dir|
          dir.each do |entry|
            return true if entry.end_with?(Chunk::SUFFIX)
          end
        end
        false
      end

      def load
        Pathname.glob("#{@path}/*#{Chunk::SUFFIX}").collect do |chunk_path|
          Chunk.load(chunk_path)
        end
      end
    end

    class Chunk
      SUFFIX = ".chunk"

      if ObjectSpace.const_defined?(:WeakMap)
        @@data_map = ObjectSpace::WeakMap.new
      else
        # XXX Ruby 1.9.x (installed by default on Ubuntu 14.0.4LTS etc.)
        #     doesn't have WeakMap...
        class WeakMap
          def initialize
            @ids = {}
          end

          def [](key)
            value_object_id = @ids[key.object_id]
            if value_object_id
              begin
                ObjectSpace._id2ref(value_object_id)
              rescue RangeError
                nil
              end
            else
              nil
            end
          end

          def []=(key, value)
            @ids[key.object_id] = value.object_id
          end
        end

        @@data_map = WeakMap.new
      end

      class << self
        def load(path)
          data_directory = path.dirname
          time_stamp1, time_stamp2, revision, = path.basename.to_s.split(".", 4)
          data = path.open("rb") {|file| file.read}
          time_stamp = Time.iso8601("#{time_stamp1}.#{time_stamp2}")
          revision = Integer(revision)
          new(data_directory, data, time_stamp, revision)
        end
      end

      attr_reader :time_stamp
      def initialize(data_directory, data, time_stamp, revision)
        @data_directory = data_directory
        @data = data
        @time_stamp = time_stamp.utc
        @revision = revision
      end

      def buffering
        path.open("wb") do |file|
          file.write(@data)
        end
        @@data_map[self] = @data
        @data = nil
      end

      def data
        @data ||= @@data_map[self] ||= read_data
      end

      def written
        FileUtils.rm_f(path.to_s)
      end

      def written_partial(size)
        written
        @data = data[size..-1]
        @revision += 1
        buffering
      end

      private
      def path
        @data_directory + "#{@time_stamp.iso8601(6)}.#{@revision}.chunk"
      end

      def read_data
        path.open("rb") do |file|
          file.read
        end
      end
    end
  end
end
