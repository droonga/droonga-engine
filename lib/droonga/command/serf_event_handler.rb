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
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

require "optparse"
require "pathname"
require "json"
require "fileutils"
require "tempfile"

require "droonga/path"
require "droonga/serf"

module Droonga
  module Command
    class SerfEventHandler
      class << self
        def run
          new.run
        end
      end

      def initialize
        @serf = ENV["SERF"] || Serf.path
        @serf_port = ENV["SERF_PORT"] || Serf.default_port
        @serf_rpc_address = ENV["SERF_RPC_ADDRESS"] || "127.0.0.1:#{@serf_port}"
      end

      def run
        parse_event
        process_event

        output_live_nodes
        true
      end

      private
      def parse_event
        @event_name = ENV["SERF_EVENT"]
        @payload = nil
        case @event_name
        when "user"
          @event_name += ":#{ENV["SERF_USER_EVENT"]}"
          @payload = $stdin.gets
        when "query"
          @event_name += ":#{ENV["SERF_USER_QUERY"]}"
          @payload = $stdin.gets
        end
      end

      def process_event
        if @event_name == "user:change_port" or
           @event_name == "query:change_port"
          serf_port = @payload.to_i
          output_port_file(serf_port)
        end
      end

      def live_nodes
        nodes = {}
        members = `#{@serf} members -rpc-addr #{@serf_rpc_address}`
        members.each_line do |member|
          name, address, status, = member.strip.split(/\s+/)
          if status == "alive"
            nodes[name] = {
              "serfAddress" => address,
            }
          end
        end
        nodes
      end

      def output_live_nodes
        path = Path.live_nodes
        nodes = live_nodes
        file_contents = JSON.pretty_generate(nodes)
        output(path, file_contents)
      end

      def output_port_file(port)
        output(Serf.port_file, port)
      end

      def output(path, file_contents)
        FileUtils.mkdir_p(path.parent.to_s)
        # Don't output the file directly to prevent loading of incomplete file!
        Tempfile.open(path.basename.to_s, path.parent.to_s, "w") do |output|
          output.write(file_contents)
          output.flush
          File.rename(output.path, path.to_s)
        end
      end
    end
  end
end
