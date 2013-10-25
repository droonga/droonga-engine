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
require "fileutils"

require "groonga"

require "droonga/plugin/handler_watch"

class StubWorker
  attr_reader :context
  def initialize(context)
    @context = context
  end
end

class ScanBenchmark
  def initialize(n_times)
    @n_times = n_times
    setup
  end

  def setup
    setup_database
    @context = Groonga::Context.new
    @context.open_database("#{@database_path}/db")
    @worker = StubWorker.new(@context)
    @watch = Droonga::WatchHandler.new(@worker)
    @hits = []
  end

  def setup_database
    @database_path = "/tmp/watch-benchmark"
    @ddl_path = File.expand_path(File.join(__FILE__, "..", "benchmark-watch-ddl.grn"))
    FileUtils.rm_rf(@database_path)
    FileUtils.mkdir_p(@database_path)
    `cat #{@ddl_path} | groonga -n #{@database_path}/db`
  end

  def run
    @n_times.times do
      scan("This is a comment.")
    end
  end

  def scan(target)
    @watch.send(:scan_body, @hits, target)
    @hits.clear
  end
end

Benchmark.bmbm do |benchmark|
  scan_benchmark = ScanBenchmark.new(100)
  benchmark.report("TODO: LABEL") do
    scan_benchmark.run
  end
end
