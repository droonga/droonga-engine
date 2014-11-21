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

require "open3"

module Droonga
  class DataAbsorber
    DEFAULT_MESSAGES_PER_SECOND = 100

    class << self
      def absorb(params)
        drndump = params[:drndump] || "drndump"
        drndump_options = []
        drndump_options += ["--host", params[:source_host]] if params[:source_host]
        drndump_options += ["--port", params[:port].to_s] if params[:port]
        drndump_options += ["--tag", params[:tag]] if params[:tag]
        drndump_options += ["--dataset", params[:dataset]] if params[:dataset]
        drndump_options += ["--receiver-host", params[:destination_host]]
        drndump_options += ["--receiver-port", params[:receiver_port].to_s] if params[:receiver_port]

        #TODO: We should use droonga-send instead of droonga-request,
        #      because droonga-request is too slow.
        #      However, to do it, we have to implement an API to know
        #      that all messages sent by droonga-send are completely
        #      processed.
        client = params[:client] || "droonga-request"
        client_options = []
        if client.include?("droonga-request")
          client_options += ["--host", params[:destination_host]]
          client_options += ["--port", params[:port].to_s] if params[:port]
          client_options += ["--tag", params[:tag]] if params[:tag]
          client_options += ["--receiver-host", params[:destination_host]]
          client_options += ["--receiver-port", params[:receiver_port].to_s] if params[:receiver_port]
        elsif client.include?("droonga-send")
          #XXX Don't use round-robin with multiple endpoints
          #    even if there are too much data.
          #    Schema and indexes must be sent to just one endpoint
          #    to keep their order, but currently there is no way to
          #    extract only schema and indexes via drndump.
          #    So, we always use just one endpoint for now,
          #    even if there are too much data.
          server = "droonga:#{params[:destination_host]}"
          server = "#{server}:#{params[:port].to_s}" if params[:port]
          server = "#{server}/#{params[:tag].to_s}" if params[:tag]
          client_options += ["--server", server]
          #XXX We should restrict the traffic to avoid overflowing!
          params[:messages_per_second] ||= DEFAULT_MESSAGES_PER_SECOND
          client_options += ["--messages-per-second", params[:messages_per_second]]
        else
          raise ArgumentError.new("Unknwon type client: #{client}")
        end

        drndump_command_line = [drndump] + drndump_options
        client_command_line = [client] + client_options

        env = {}
        Open3.pipeline_r([env, *drndump_command_line],
                         [env, *client_command_line]) do |last_stdout, thread|
          last_stdout.each do |output|
            yield output if block_given?
          end
        end
      end
    end
  end
end
