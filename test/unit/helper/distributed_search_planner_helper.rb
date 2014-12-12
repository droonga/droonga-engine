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

require "droonga/plugins/search/distributed_search_planner"

module DistributedSearchPlannerHelper
  def plan(search_request)
    # TODO: Use real dataset
    stub_dataset = Object.new
    stub(stub_dataset).name do
      Droonga::Catalog::Dataset::DEFAULT_NAME
    end
    stub(stub_dataset).sliced? do
      true
    end
    planner = Droonga::Plugins::Search::DistributedSearchPlanner.new(stub_dataset, search_request)
    planner.plan
  end

  def messages
    @messages ||= plan(@request)
  end

  def broadcast_message
    messages.find do |message|
      message["type"] == "broadcast"
    end
  end

  def reduce_message
    messages.find do |message|
      message["type"] == "search_reduce"
    end
  end

  def gather_message
    messages.find do |message|
      message["type"] == "search_gather"
    end
  end

  def dependencies
    dependencies = messages.collect do |message|
      {
        "type"    => message["type"],
        "inputs"  => message["inputs"],
        "outputs" => message["outputs"],
      }
    end
    sort_dependencies(dependencies)
  end

  def sort_dependencies(dependencies)
    dependencies.sort do |a, b|
      a["type"] <=> b["type"]
    end
  end

  def expected_dependencies(reduce_inputs, gather_inputs)
    dependencies = [
      {
        "type"    => "search_reduce",
        "inputs"  => reduce_inputs,
        "outputs" => gather_inputs,
      },
      {
        "type"    => "search_gather",
        "inputs"  => gather_inputs,
        "outputs" => nil,
      },
      {
        "type"    => "broadcast",
        "inputs"  => nil,
        "outputs" => reduce_inputs,
      },
    ]
    sort_dependencies(dependencies)
  end
end
