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
require File.expand_path(File.join(__FILE__, "..", "..", "utils.rb"))

class ScanBenchmark
  def initialize(n_times)
    @n_times = n_times

    @database = WatchDatabase.new

    @worker = DroongaBenchmark::StubWorker.new(@database.context)
    @watch_handler = Droonga::WatchHandler.new(@worker)

    @terms = DroongaBenchmark::TermsGenerator.generate(@n_times)
    @targets = DroongaBenchmark::TargetsGenerator.generate(@n_times,
                                                           :terms => @terms,
                                                           :incidence => 0.1)

    @terms.each do |term|
      @database.subscribe(term)
    end

    @hits = []
  end

  def run
    @targets.each do |target|
      scan(target)
    end
  end

  def scan(target)
    @watch_handler.send(:scan_body, @hits, target)
    @hits.clear
  end
end

Benchmark.bmbm do |benchmark|
  scan_benchmark = ScanBenchmark.new(100)
  benchmark.report("TODO: LABEL") do
    scan_benchmark.run
  end
end
