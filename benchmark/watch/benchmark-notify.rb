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

# this benchmark must be done by benchmark-notify.sh.

require "benchmark"
require "fileutils"
require "optparse"
require "csv"
require "json"

require "droonga/client"

require File.expand_path(File.join(__FILE__, "..", "..", "utils.rb"))

class NotifyBenchmark
  attr_reader :n_subscribers

  WATCHING_KEYWORD = "a"

  def initialize(params)
    @params = params || {}
    @n_times = params[:n_times] || 0
    @timeout = params[:timeout] || 0

    @n_subscribers = 0

    @client = Droonga::Client.new(tag: "droonga", port: 23003)
    @receiver = Droonga::Client::Connection::DroongaProtocol::Receiver.new
    @route = "#{@receiver.host}:#{@receiver.port}/droonga"
    setup
  end

  def setup
    ddl_path = File.expand_path(File.join(__FILE__, "..", "..", "..", "ddl", "watchdb.jsons"))
    ddl_jsons = File.read(ddl_path)
    ddl_jsons.split("\n").each do |part|
      message = JSON.parse(part)
      @client.connection.send_receive(message)
    end
    add_subscribers(@params[:n_initial_subscribers])
  end

  def run
    @n_times.times do
      do_feed(WATCHING_KEYWORD)
    end
    p @receiver.receive(:timeout => @timeout)
  end

  def add_subscribers(n_subscribers)
    n_subscribers.times do
      message = DroongaBenchmark::MessageCreator.envelope_to_subscribe(WATCHING_KEYWORD, @route)
      @client.connection.send(message)
    end
    @n_subscribers += n_subscribers
  end

  def do_feed(target)
    message = DroongaBenchmark::MessageCreator.envelope_to_feed(target)
    @client.connection.send(message)
  end
end

options = {
  :n_subscribers => 1000,
  :n_times       => 1000,
  :n_steps       => 10,
  :output_path   => "/tmp/watch-benchmark-notify.csv",
}
option_parser = OptionParser.new do |parser|
  parser.on("--subscribers=N", Integer,
            "initial number of subscribers (optional)") do |n_subscribers|
    options[:n_subscribers] = n_subscribers
  end
  parser.on("--times=N", Integer,
            "number of publish times (optional)") do |n_times|
    options[:n_times] = n_times
  end
  parser.on("--steps=N", Integer,
            "number of benchmark steps (optional)") do |n_steps|
    options[:n_steps] = n_steps
  end
  parser.on("--timeout=N", Float,
            "timeout for receiving (optional)") do |timeout|
    options[:timeout] = timeout
  end
  parser.on("--output-path=PATH", String,
            "path to the output CSV file (optional)") do |output_path|
    options[:output_path] = output_path
  end
end
args = option_parser.parse!(ARGV)


notify_benchmark = NotifyBenchmark.new(:n_initial_subscribers => options[:n_subscribers],
                                       :n_times => options[:n_times],
                                       :timeout => options[:timeout])
results = []
options[:n_steps].times do |try_count|
  notify_benchmark.add_subscribers(notify_benchmark.n_subscribers) if try_count > 0
  label = "#{notify_benchmark.n_subscribers} subscribers"
  result = Benchmark.bmbm do |benchmark|
    benchmark.report(label) do
      notify_benchmark.run
    end
  end
  result = result.join("").strip.gsub(/[()]/, "").split(/\s+/)
  results << [label] + result
end
total_results = [
  ["case", "user", "system", "total", "real"],
]
total_results += results

puts ""
puts "Results (saved to #{options[:output_path]}):"
File.open(options[:output_path], "w") do |file|
  total_results.each do |row|
    file.puts(CSV.generate_line(row))
    puts row.join(",")
  end
end
