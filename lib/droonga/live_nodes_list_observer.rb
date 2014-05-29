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

require "fileutils"
require "listen"

require "droonga/path"
require "droonga/loggable"
require "droonga/live_nodes_list_loader"

module Droonga
  class LiveNodesListObserver
    class << self
      FILE_NAME = "live-nodes.json"

      def path
        Path.state + FILE_NAME
      end
    end

    include Loggable

    attr_accessor :on_update

    def initialize
    end

    def start
      path = self.class.path
      file_name = path.expand_path.to_s
      directory = path.dirname.to_s
      FileUtils.mkdir_p(directory)
      @listener = Listen.to(directory) do |modified, added, removed|
        if added.include?(file_name) or
             modified.include?(file_name)
          load_list!
        end
      end
      @listener.start
    end

    def stop
      @listener.stop
    end

    def load_list!
      path = self.class.path
      loader = LiveNodesListLoader.new(path)
      live_nodes = loader.load
      logger.info("loaded", :path => path.to_s, :live_nodes => live_nodes)

      on_update.call(live_nodes) if on_update
    end

    private
    def log_tag
      "live-nodes-list-observer"
    end
  end
end
