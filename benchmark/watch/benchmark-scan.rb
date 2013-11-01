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
  attr_reader :n_keywords

  def initialize(n_times, options={})
    @n_times = n_times
    @incidence = options[:incidence]

    @database = DroongaBenchmark::WatchDatabase.new

    @watcher = Droonga::Watcher.new(@database.context)

    @keywords_generator = DroongaBenchmark::KeywordsGenerator.new
    @keywords = @keywords_generator.generate(@n_times)
    prepare_targets(options)

    @database.subscribe_to(@keywords)
    @n_keywords = @keywords.size

    @hits = []
  end

  def run
    @targets.each do |target|
      scan(target)
    end
  end

  def prepare_targets(options={})
    @incidence = options[:incidence] || 0
    @matched_keywords = options[:matched_keywords] || 0
    @targets = DroongaBenchmark::TargetsGenerator.generate(@n_times,
                                                           :keywords => @keywords.sample(@n_times),
                                                           :incidence => @incidence,
                                                           :matched_keywords => @matched_keywords)
  end

  def add_keywords(n_keywords)
    new_keywords = []
    n_keywords.times do
      new_keywords << @keywords_generator.next
    end
    @database.subscribe_to(new_keywords)
    @keywords += new_keywords
    @n_keywords += n_keywords
  end

  def memory_usage
    /^VmRSS:\s*(\d+) kB/ =~ File.read("/proc/self/status")
      $1.to_i * 1024
    end
  end

  private
  def scan(target)
    @watcher.scan_body(@hits, target)
    @hits.clear
  end
end

options = {
  :n_watching_keywords => 1000,
  :n_steps          => 10,
  :incidences       => "0.1,0.5,0.9",
  :matched_keywords => "1,5,10",
  :output_path      => "/tmp/watch-benchmark-scan",
}
option_parser = OptionParser.new do |parser|
  parser.on("--keywords=N", Integer,
            "number of watching keywords") do |n_watching_keywords|
    options[:n_watching_keywords] = n_watching_keywords
  end
  parser.on("--steps=N", Integer,
            "number of benchmark steps") do |n_steps|
    options[:n_steps] = n_steps
  end
  parser.on("--incidences=INCIDENCES", String,
            "list of matching incidences") do |incidences|
    options[:incidences] = incidences
  end
  parser.on("--matched-keywords=MATCHED_KEYWORDS", String,
            "number of keywords which is matched per a target") do |matched_keywords|
    options[:matched_keywords] = matched_keywords
  end
  parser.on("--output-path=PATH", String,
            "path to the output CSV file") do |output_path|
    options[:output_path] = output_path
  end
end
args = option_parser.parse!(ARGV)

results_for_specific_condition = {}
scan_benchmark = ScanBenchmark.new(options[:n_watching_keywords])
options[:n_steps].times do |try_count|
  scan_benchmark.add_keywords(scan_benchmark.n_keywords) if try_count > 0
  GC.start
  sleep 1
  puts "\n=============== #{scan_benchmark.n_keywords} keywords ===============\n"
  options[:incidences].split(/[,\s]+/).each do |incidence|
    incidence = incidence.to_f
    options[:matched_keywords].split(/[,\s]+/).each do |matched_keywords|
      matched_keywords = matched_keywords.to_i

      GC.disable

      condition = "#{incidence * 100}%/#{matched_keywords}match"
      results_for_specific_condition[condition] ||= []
      label = "#{incidence * 100} %/#{matched_keywords} match/#{scan_benchmark.n_keywords} keywords"

      result = Benchmark.bmbm do |benchmark|
        scan_benchmark.prepare_targets(:incidence => incidence,
                                       :matched_keywords => matched_keywords)
        benchmark.report(label) do
          scan_benchmark.run
        end
      end

      result = result.join("").strip.gsub(/[()]/, "").split(/\s+/)
      result << scan_benchmark.memory_usage
      results_for_specific_condition[condition] << [label] + result

      GC.enable
      GC.start
      sleep 1
    end
  end
end

FileUtils.mkdir_p(options[:output_path])

puts ""
all_output = File.join(options[:output_path], "all.csv")
all_results = [
  ["case", "user", "system", "total", "real"],
]
results_for_specific_condition.values.each do |results|
  all_results += results
end
puts "All (saved to #{all_output}):"
File.open(all_output, "w") do |file|
  all_results.each do |row|
    file.puts(CSV.generate_line(row))
    puts row.join(",")
  end
end

puts ""
total_output = File.join(options[:output_path], "total.csv")
total_results_header = ["case"]
total_results = []
results_for_specific_condition.each do |condition, results|
  total_results_header << condition
  results.each_index do |index|
    total_results[index] ||= [results[index].first.split("/").last]
    total_results[index] << results[index][3]
  end
end
total_results.unshift(total_results_header)
puts "Total (saved to #{total_output}):"
File.open(total_output, "w") do |file|
  total_results.each do |row|
    file.puts(CSV.generate_line(row))
    puts row.join(",")
  end
end

puts ""
real_output = File.join(options[:output_path], "real.csv")
real_results_header = ["case"]
real_results = []
results_for_specific_condition.each do |condition, results|
  real_results_header << condition
  results.each_index do |index|
    real_results[index] ||= [results[index].first.split("/").last]
    real_results[index] << results[index][4]
  end
end
real_results.unshift(real_results_header)
puts "Real (saved to #{real_output}):"
File.open(real_output, "w") do |file|
  real_results.each do |row|
    file.puts(CSV.generate_line(row))
    puts row.join(",")
  end
end
