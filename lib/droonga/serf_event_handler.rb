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

    attr_writer :live_nodes

    def run(command_line_arguments)
      parse_command_line_arguments!(command_line_arguments)
      parse_event

      update_live_nodes
      output_live_nodes
      0
    end

    def changed_nodes
      @changed_nodes ||= parse_changed_nodes(@payload)
    end

    def live_nodes
      @live_nodes ||= load_live_nodes(@list_file)
    end

    private
    def parse_command_line_arguments!(command_line_arguments)
      parser = OptionParser.new

      parser.on("--list-file=FILE",
                "Output list of live nodes to FILE") do |file|
        @list_file = Pathname(file)
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

      @payload = STDIN.read
    end

    def parse_changed_nodes(payload)
      nodes = {}
      payload.each_line do |node|
        name, address, role, tags = node.strip.split(/\s+/)
        nodes[name] = {
          "address" => address,
          "role"    => role,
          "tags"    => tags,
        }
      end
      nodes
    end

    def load_live_nodes(file)
      nodes = {}
      if file
        contents = file.read
        nodes = JSON.parse(contents) if contents and not contents.empty?
      end
      nodes
    rescue StandardError, LoadError, SyntaxError => error
      {}
    end

    def update_live_nodes
      case @event_name
      when "member-join"
        nodes = live_nodes
        live_nodes = nodes.merge(changed_nodes)
      when "member-leave", "member-failed"
        changed_nodes.each do |name, attributes|
          live_nodes.delete(name)
        end
      # when "user:XXX", "query:XXX"
      end
    end

    def output_live_nodes
      list_file_contents = JSON.pretty_generate(live_nodes)
      if @list_file
        @list_file.write(list_file_contents)
      else
        puts list_file_contents
      end
    end
  end
end
