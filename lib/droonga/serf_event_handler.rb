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

module Droonga
  class SerfEventHandler
    class << self
      def run(command_line_arguments)
        new.run(command_line_arguments)
      end
    end

    def initialize
      @serf_command = "serf"
    end

    def run(command_line_arguments)
      parse_command_line_arguments!(command_line_arguments)
      parse_event

      output_live_nodes
      true
    end

    private
    def parse_command_line_arguments!(command_line_arguments)
      parser = OptionParser.new

      parser.on("--live-nodes-file=FILE",
                "Output list of live nodes to FILE") do |file|
        @live_nodes_file = Pathname(file)
      end
      parser.on("--serf-command=FILE",
                "Path to the serf command") do |file|
        @serf_command = file
      end

      parser.parse!(command_line_arguments)
    end

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
      members = system(@serf_command, "members")
      members.each_line do |member|
        name, address, status, = member.strip.split(/\s+/)
        if status == "alive"
          nodes[name] = {
            "address" => address,
          }
        end
      end
      nodes
    end

    def output_live_nodes
      nodes = live_nodes
      file_contents = JSON.pretty_generate(nodes)
      if @live_nodes_file
        @live_nodes_file.write(file_contents)
      else
        puts file_contents
      end
    end
  end
end
