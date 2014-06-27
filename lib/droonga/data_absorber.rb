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

require "open3"

module Droonga
  class DataAbsorber
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

        client = params[:client] || "droonga-request"
        client_options = []
        client_options += ["--host", params[:destination_host]]
        client_options += ["--port", params[:port].to_s] if params[:port]
        client_options += ["--tag", params[:tag]] if params[:tag]
        client_options += ["--receiver-host", params[:destination_host]]
        client_options += ["--receiver-port", params[:receiver_port].to_s] if params[:receiver_port]

        drndump_command_line = [drndump] + drndump_options
        client_command_line = [client] + client_options

        Open3.popen3(*drndump_command_line) do |dump_in, dump_out, dump_error, dump_thread|
          dump_in.close
          Open3.popen3(*client_command_line) do |client_in, client_out, client_error, client_thread|
            client_out.close
            dump_out.each do |dump|
              yield dump if block_given?
              client_in.puts(dump)
            end
          end
        end
      end
    end
  end
end
