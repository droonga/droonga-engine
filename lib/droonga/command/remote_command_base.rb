#!/usr/bin/env ruby
#
# Copyright (C) 2015 Droonga Project
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

require "slop"

require "droonga/engine/version"
require "droonga/node_name"
require "droonga/serf"

module Droonga
  module Command
    class RemoteCommandBase
      private
      def parse_options(&block)
        options = Slop.parse(:help => true) do |option|
          yield(option) if block_given?

          option.separator("Connections:")
          option.on(:host=,
                    "Host name of the node to be operated.",
                    :required => true)
          option.on(:port=,
                    "Port number to communicate with the engine node.",
                    :as => Integer,
                    :default => NodeName::DEFAULT_PORT)
          option.on(:tag=,
                    "Tag name to communicate with the engine node.",
                    :default => NodeName::DEFAULT_TAG)

          option.separator("Miscellaneous:")
          option.on(:verbose, "Output details for internal operations.",
                    :default => false)
        end
        @options = options
      rescue Slop::MissingOptionError => error
        $stderr.puts(error)
        exit(false)
      end

      def host
        @options[:host]
      end

      def port
        @options[:port]
      end

      def tag
        @options[:tag]
      end

      def node
        @node ||= NodeName.new(:host => host,
                               :port => port,
                               :tag  => tag)
      end

      def serf
        @serf ||= Serf.new(node.to_s,
                           :verbose => @options[:verbose])
      end
    end
  end
end
