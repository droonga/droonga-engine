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
require "droonga/live_nodes_list_observer"

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
        @serf_rpc_address = ENV["SERF_RPC_ADDRESS"] || "127.0.0.1:7373"
      end

      def run
        parse_event

        output_live_nodes
        true
      end

      private
      def parse_event
        @event_name = ENV["SERF_EVENT"]
        case @event_name
        when "user"
          @event_name += ":#{ENV["SERF_USER_EVENT"]}"
        when "query"
          @event_name += ":#{ENV["SERF_USER_QUERY"]}"
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
        list_path = LiveNodesListObserver.path
        nodes = live_nodes
        file_contents = JSON.pretty_generate(nodes)
        # Don't output the file directly to prevent loading of incomplete file!
        Tempfile.open(list_path.basename.to_s, list_path.parent.to_s, "w") do |output|
          output.write(file_contents)
          output.flush
          File.rename(output.path, list_path.to_s)
        end
      end
    end
  end
end
