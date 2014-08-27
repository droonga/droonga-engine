# -*- mode: ruby; coding: utf-8 -*-
#
# Copyright (C) 2013-2014 Droonga Project
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

base_dir = File.dirname(__FILE__)
$LOAD_PATH.unshift(File.join(base_dir, "lib"))

require "droonga/engine/version"

Gem::Specification.new do |gem|
  gem.name          = "droonga-engine"
  gem.version       = Droonga::Engine::VERSION
  gem.authors       = ["Droonga Project"]
  gem.email         = ["droonga@groonga.org"]
  gem.summary       = "Droonga engine"
  gem.description   =
    "Droonga engine is a core component in Droonga system. " +
    "Droonga is a scalable data processing engine based on Groonga. " +
    "Droonga means Distributed Groonga."
  gem.homepage      = "https://github.com/droonga/droonga-engine"
  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]
  gem.add_dependency "droonga-client"
  gem.add_dependency "rroonga", ">= 4.0.1"
  gem.add_dependency "groonga-command-parser"
  gem.add_dependency "json"
  gem.add_dependency "cool.io"
  gem.add_dependency "droonga-message-pack-packer", ">= 1.0.1"
  gem.add_dependency "faraday"
  gem.add_dependency "faraday_middleware"
  gem.add_dependency "archive-zip"
  gem.add_dependency "sigdump"
  gem.add_dependency "droonga-client", ">= 0.1.9"
  gem.add_dependency "drndump"
  gem.add_dependency "slop"
  gem.add_development_dependency "rake"
  gem.add_development_dependency "bundler"
  gem.add_development_dependency "test-unit"
  gem.add_development_dependency "test-unit-notify"
  gem.add_development_dependency "test-unit-rr"
  gem.add_development_dependency "packnga"
  gem.add_development_dependency "kramdown"
end
