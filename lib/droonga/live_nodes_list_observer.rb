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
require "droonga/loggable"
require "droonga/live_nodes_list_loader"

module Droonga
  class LiveNodesListObserver
    include Loggable

    attr_accessor :on_update

    def initialize
      FileUtils.mkdir_p(directory_path)
      @listener = Listen.to(directory_path) do |modified, added, removed|
        if added.include?(file_path) or
             modified.include?(file_path)
          load_list!
        end
      end
    end

    def start
      @listener.start
    end

    def stop
      @listener.stop
    end

    LIST_FILE_NAME = "list.json"
    OBSERVE_DIR_NAME = "live-nodes"
    DEFAULT_LIST_PATH = "#{OBSERVE_DIR_NAME}/#{LIST_FILE_NAME}"

    def base_path
      ENV["DROONGA_BASE_DIR"]
    end

    def file_path
      @file_path ||= prepare_file_path
    end

    def directory_path
      File.dirname(file_path)
    end

    def load_list!
      loader = LiveNodesListLoader.new(file_path)
      live_nodes = loader.load
      logger.info("loaded", :path => file_path, :live_nodes => live_nodes)

      on_update.call(live_nodes) if on_update
    end

    private
    def prepare_file_path
      path = ENV["DROONGA_LIVE_NODES_LIST"] || DEFAULT_LIST_PATH
      File.expand_path(path, base_path)
    end

    def log_tag
      "live-nodes-list-observer"
    end
  end
end
