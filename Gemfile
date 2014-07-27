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

source "https://rubygems.org"

gemspec

parent_dir = File.join(File.dirname(__FILE__), "..")
local_rroonga_path = File.join(parent_dir, "rroonga")
local_groonga_command_path = File.join(parent_dir, "groonga-command")
local_groonga_command_parser_path = File.join(parent_dir,
                                              "groonga-command-parser")
if File.exist?(local_rroonga_path)
  gem "rroonga", :path => local_rroonga_path
  gem "groonga-command", :path => local_groonga_command_path
  gem "groonga-command-parser", :path => local_groonga_command_parser_path
elsif ENV["TRAVIS"] == "true"
  require_unreleased_gems = true
  if require_unreleased_gems
    gem "rroonga", :git => "git://github.com/ranguba/rroonga.git"
    gem "groonga-command",
        :git => "git://github.com/groonga/groonga-command.git"
    gem "groonga-command-parser",
        :git => "git://github.com/groonga/groonga-command-parser.git"
  end
end

droonga_message_pack_packer_dir = File.join(parent_dir, "droonga-message-pack-packer-ruby")
if File.exist?(droonga_message_pack_packer_dir)
  gem "droonga-message-pack-packer", :path => droonga_message_pack_packer_dir
else
  gem "droonga-message-pack-packer", :github => "droonga/droonga-message-pack-packer-ruby"
end

droonga_client_dir = File.join(parent_dir, "droonga-client-ruby")
if File.exist?(droonga_client_dir)
  gem "droonga-client", :path => droonga_client_dir
else
  gem "droonga-client", :github => "droonga/droonga-client-ruby"
end

drntest_dir = File.join(parent_dir, "drntest")
if File.exist?(drntest_dir)
  gem "drntest", :path => drntest_dir
else
  gem "drntest", :github => "droonga/drntest"
end

drnbench_dir = File.join(parent_dir, "drnbench")
if File.exist?(drnbench_dir)
  gem "drnbench", :path => drnbench_dir
else
  gem "drnbench", :github => "droonga/drnbench"
end

grn2drn_dir = File.join(parent_dir, "grn2drn")
if File.exist?(grn2drn_dir)
  gem "grn2drn", :path => grn2drn_dir
else
  gem "grn2drn", :github => "droonga/grn2drn"
end

drndump_dir = File.join(parent_dir, "drndump")
if File.exist?(drndump_dir)
  gem "drndump", :path => drndump_dir
else
  gem "drndump", :github => "droonga/drndump"
end
