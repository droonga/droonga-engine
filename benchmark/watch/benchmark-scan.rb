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

require "droonga/watcher"
require File.expand_path(File.join(__FILE__, "..", "..", "utils.rb"))

class ScanBenchmark
  attr_reader :n_terms

  def initialize(n_times, incidence)
    @n_times = n_times
    @incidence = incidence

    @database = DroongaBenchmark::WatchDatabase.new

    @watcher = Droonga::Watcher.new(@database.context)

    @terms_generator = DroongaBenchmark::TermsGenerator.new
    @terms = @terms_generator.generate(@n_times)
    @targets = DroongaBenchmark::TargetsGenerator.generate(@n_times,
                                                           :terms => @terms,
                                                           :incidence => @incidence)

    @terms.each do |term|
      @database.subscribe(term)
    end
    @n_terms = @terms.size

    @hits = []
  end

  def run
    @targets.each do |target|
      scan(target)
    end
  end

  def add_terms(n_terms)
    n_terms.times do
      @database.subscribe(@terms_generator.next)
    end
    @n_terms += n_terms
  end

  private
  def scan(target)
    @watcher.scan_body(@hits, target)
    @hits.clear
  end
end

n_watching_terms = 1000
step             = 1000
n_tests          = 20
incidences       = [0.1, 0.5, 0.9]
Benchmark.bmbm do |benchmark|
  puts "starting..."
  incidences.each do |incidence|
    puts "preparing bencharmk for incidence #{incidence}..."
    scan_benchmark = ScanBenchmark.new(n_watching_terms, incidence)
    n_tests.times do |try_count|
      scan_benchmark.add_terms(step) if try_count > 0
      benchmark.report("incidence #{incidence}, #{scan_benchmark.n_terms} keywords") do
        scan_benchmark.run
      end
    end
  end
end
