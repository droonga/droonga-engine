#!/usr/bin/env ruby
#
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

require "rbconfig"
require "fileutils"

def run(*command_line)
  command_line = command_line.collect do |argument|
    argument.to_s
  end
  return if system(*command_line)
  puts("failed to run: #{command_line.join(' ')}")
  exit(false)
end

base_dir = File.dirname(__FILE__)
lib_dir = File.expand_path(File.join(base_dir, "..", "..", "lib"))

drnbench_options = []
drnbench_options.concat(["--start-n-subscribers", 1000])
drnbench_options.concat(["--n-publishings", 1000])
drnbench_options.concat(["--n-steps", 10])
drnbench_options.concat(["--timeout", 5])
drnbench_options.concat(["--subscribe-request-file",
                         File.join(base_dir, "watch", "subscribe.json")])
drnbench_options.concat(["--feed-file",
                         File.join(base_dir, "watch", "feed.json")])

protocol_adapter_dir = File.join(base_dir, "..", "..", "..", "express-droonga")
if File.exist?(protocol_adapter_dir)
  drnbench_options.concat(["--protocol-adapter-application-dir", protocol_adapter_dir])
  drnbench_options.concat(["--protocol-adapter-port", 13000])
end

drnbench_options.concat(["--engine-config-path",
                         File.join(base_dir, "watch")])
drnbench_options.concat(["--fluentd-options", "-I#{lib_dir}"])
drnbench_options.concat(ARGV)

drnbench_publish_subscribe = Gem.bin_path("drnbench",
                                          "drnbench-publish-subscribe")
run(drnbench_publish_subscribe, *drnbench_options)
