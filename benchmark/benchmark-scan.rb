# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 droonga project
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

require "benchmark"

require "groonga"

require "droonga/plugin/handler_watch"

class StubWorker
  attr_reader :context
  def initialize(context)
    @context = context
  end
end

Groonga::Database.open("/tmp/watch/db")

worker = StubWorker.new(Groonga::Context.default)
watch = Droonga::WatchHandler.new(worker)

n = 100
Benchmark.bmbm do |benchmark|
  benchmark.report("") do
    hits = []
    n.times do
      watch.send(:scan_body, hits, "This is a comment.")
      hits.clear
    end
  end
end
