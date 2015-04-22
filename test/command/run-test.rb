#!/usr/bin/env ruby
#
# Copyright (C) 2013 Droonga Project
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

require "rbconfig"
require "fileutils"

def system_serf_exist?
  system("serf", "version",
         :out => "/dev/null",
         :err => "/dev/null")
end

unless system_serf_exist?
  puts("Serf is not installed to your system.")
  puts("Install Serf before running this test.")
  puts("See: http://www.serfdom.io/")
  exit(false)
end

def run(*command_line)
  return if system(*command_line)
  puts("failed to run: #{command_line.join(' ')}")
  exit(false)
end

base_dir = File.dirname(__FILE__)
lib_dir = File.expand_path(File.join(base_dir, "..", "..", "lib"))
log_path = File.expand_path(File.join(base_dir, "tmp", "droonga-engine.log"))

drntest_options = []
drntest_options.concat(["--base-path", base_dir])
drntest_options.concat(["--droonga-engine-options",
                        ["--log-file=#{log_path}",
                         "--internal-connection-lifetime=5"].join(" ")])
drntest_options.concat(ARGV)

run("bundle", "exec", "drntest", *drntest_options)
