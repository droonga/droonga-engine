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

module Droonga
  class LiveNodesList
    def initialize(nodes)
      @nodes = nodes
    end

    def all_nodes
      @all_nodes ||= @nodes.keys.sort
    end

    def dead_nodes
      @dead_nodes ||= collect_dead_nodes
    end

    def suspended_nodes
      @suspended_nodes ||= collect_suspended_nodes
    end

    def ==(nodes_list)
      nodes_list.is_a?(self.class) and
        nodes_list.all_nodes == all_nodes and
        nodes_list.dead_nodes == dead_nodes and
        nodes_list.suspended_nodes == suspended_nodes
    end

    private
    def collect_dead_nodes
      nodes = []
      @nodes.each do |name, state|
        unless state["live"]
          nodes << name
        end
      end
      nodes.sort
    end

    def collect_suspended_nodes
      nodes = []
      @nodes.each do |name, state|
        if not state["foreign"] and
             state["tags"]["suspended"] == "true"
          nodes << name
        end
      end
      nodes.sort
    end
  end
end
