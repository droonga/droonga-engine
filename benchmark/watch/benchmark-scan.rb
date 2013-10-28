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
require "optparse"
require "csv"

require "groonga"

require "droonga/watcher"
require File.expand_path(File.join(__FILE__, "..", "..", "utils.rb"))

class ScanBenchmark
  attr_reader :n_terms

  def initialize(n_times, incidence=0)
    @n_times = n_times
    @incidence = incidence

    @database = DroongaBenchmark::WatchDatabase.new

    @watcher = Droonga::Watcher.new(@database.context)

    @terms_generator = DroongaBenchmark::TermsGenerator.new
    @terms = @terms_generator.generate(@n_times)
    prepare_targets(@incidence)

    @database.subscribe_to(@terms)
    @n_terms = @terms.size

    @hits = []
  end

  def run
    @targets.each do |target|
      scan(target)
    end
  end

  def prepare_targets(incidence=0)
    @incidence = incidence
    @targets = DroongaBenchmark::TargetsGenerator.generate(@n_times,
                                                           :terms => @terms.sample(@n_times),
                                                           :incidence => @incidence)
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

options = {
  :n_watching_terms => 1000,
  :n_steps          => 10,
  :incidences       => "0.1,0.5,0.9",
  :output_path      => "/tmp/watch-benchmark-scan.csv",
}
option_parser = OptionParser.new do |parser|
  parser.on("--terms=N", Integer,
            "number of watching terms (optional)") do |n_watching_terms|
    options[:n_watching_terms] = n_watching_terms
  end
  parser.on("--steps=N", Integer,
            "number of benchmark steps (optional)") do |n_steps|
    options[:n_steps] = n_steps
  end
  parser.on("--incidences=INCIDENCES", String,
            "list of matching incidences (optional)") do |incidences|
    options[:incidences] = incidences
  end
  parser.on("--output-path=PATH", String,
            "path to the output CSV file (optional)") do |output_path|
    options[:output_path] = output_path
  end
end
args = option_parser.parse!(ARGV)


results_by_incidence = {}
scan_benchmark = ScanBenchmark.new(options[:n_watching_terms])
options[:n_steps].times do |try_count|
  scan_benchmark.add_terms(scan_benchmark.n_terms) if try_count > 0
  options[:incidences].split(/[,\s]+/).each do |incidence|
    results_by_incidence[incidence] ||= []
    scan_benchmark.prepare_targets(incidence.to_f)
    label = "incidence #{incidence}/#{scan_benchmark.n_terms} keywords"
    result = Benchmark.bmbm do |benchmark|
      benchmark.report(label) do
        scan_benchmark.run
      end
    end
    result = result.join("").strip.gsub(/[()]/, "").split(/\s+/)
    results_by_incidence[incidence] << [label] + result
  end
end
total_results = [
  ["case", "user", "system", "total", "real"],
]
results_by_incidence.values.each do |results|
  total_results += results
end

puts ""
puts "Results (saved to #{options[:output_path]}):"
File.open(options[:output_path], "w") do |file|
  total_results.each do |row|
    file.puts(CSV.generate_line(row))
    puts row.join(",")
  end
end
