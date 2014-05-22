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

require "droonga/base_path"
require "droonga/loggable"
require "droonga/catalog_observer"
require "droonga/serf_downloader"

module Droonga
  class Serf
    class << self
      def path
        @path ||= Droonga.base_path + "serf"
      end
    end

    include Loggable

    def initialize(loop, name)
      @loop = loop
      @name = name
    end

    def start
      logger.trace("start: start")
      ensure_serf
      ENV["SERF"] = self.class.path.to_s
      ENV["SERF_RPC_ADDRESS"] = rpc_address
      retry_joins = []
      detect_other_hosts.each do |other_host|
        retry_joins.push("--retry-join", other_host)
      end
      @serf_pid = run("agent",
                      "-node", @name,
                      "-bind", extract_host(@name),
                      "-event-handler", "#{$0}-serf-event-handler",
                      *retry_joins)
      logger.trace("start: done")
    end

    def shutdown
      logger.trace("shutdown: start")
      Process.waitpid(run("leave"))
      Process.waitpid(@serf_pid)
      logger.trace("shutdown: done")
    end

    private
    def ensure_serf
      serf_path = self.class.path
      return if serf_path.executable?
      downloader = SerfDownloader.new(serf_path)
      downloader.download
    end

    def run(command, *options)
      spawn(self.class.path.to_s, command, "-rpc-addr", rpc_address, *options)
    end

    def extract_host(node_name)
      node_name.split(":").first
    end

    def rpc_address
      "#{extract_host(@name)}:7373"
    end

    def detect_other_hosts
      catalog_observer = Droonga::CatalogObserver.new(@loop)
      catalog = catalog_observer.catalog
      other_nodes = catalog.all_nodes.reject do |node|
        node == @name
      end
      other_nodes.collect do |node|
        extract_host(node)
      end
    end

    def log_tag
      "serf"
    end
  end
end
