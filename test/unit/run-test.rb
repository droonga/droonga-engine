#!/usr/bin/env ruby
# -*- coding: utf-8 -*-
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

require "pathname"

require "rubygems"
require "bundler"
begin
  Bundler.setup(:default, :development)
rescue Bundler::BundlerError => e
  $stderr.puts e.message
  $stderr.puts "Run `bundle install` to install missing gems"
  exit e.status_code
end

require "test-unit"
require "test/unit/notify"
require "test/unit/rr"

require "cool.io"

$VERBOSE = true

base_dir = File.expand_path(File.join(File.dirname(__FILE__), "..", ".."))
lib_dir = File.join(base_dir, "lib")
test_dir = File.join(base_dir, "test", "unit")

$LOAD_PATH.unshift(lib_dir)

require "droonga/engine"
require "droonga/plugin_loader"

Droonga::PluginLoader.load_all

$LOAD_PATH.unshift(test_dir)

require "helper"

ARGV.unshift("--max-diff-target-string-size=10000")

exit Test::Unit::AutoRunner.run(true, test_dir)
