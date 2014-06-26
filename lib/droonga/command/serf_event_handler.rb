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
        @serf_rpc_address = ENV["SERF_RPC_ADDRESS"] || "127.0.0.1:7373"
      end

      def run
        parse_event

        output_nodes_status
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

      def parse_tags(tags)
        parsed = {}
        return parsed unless tags

        tags.split(",").each do |tag|
          key, value = tag.split("=")
          parsed[key] = value
        end
        parsed
      end

      def nodes_status
        nodes_status = {}
        members = `#{@serf} members -rpc-addr #{@serf_rpc_address}`
        members.each_line do |member|
          name, address, status, tags, = member.strip.split(/\s+/)
          nodes_status[name] = {
            "serfAddress" => address,
            "live"        => status == "alive",
            "tags"        => parse_tags(tags),
          }
        end
        nodes_status
      end

      def output_nodes_status
        path = Path.nodes_status
        status = nodes_status
        file_contents = JSON.pretty_generate(status)
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
