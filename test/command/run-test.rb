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
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

require "rbconfig"
require "fileutils"

def run(*command_line)
  return if system(*command_line)
  puts("failed to run: #{command_line.join(' ')}")
  exit(false)
end

def need_bundle_install?(gemfile, gemfile_lock)
  return true unless File.exist?(gemfile_lock)
  return true if File.mtime(gemfile) > File.mtime(gemfile_lock)
  false
end

base_dir = File.dirname(__FILE__)
gemfile = File.join(base_dir, "Gemfile")
gemfile_lock = "#{gemfile}.lock"

if need_bundle_install?(gemfile, gemfile_lock)
  Dir.chdir(base_dir) do
    run("bundle", "install", "--binstubs")
  end
end

ENV["BUNDLE_GEMFILE"] = File.expand_path(gemfile)

drntest_options = ARGV.dup
drntest_options << File.join(base_dir, "suite")

run(File.join(base_dir, "bin", "drntest"),
    *drntest_options)
