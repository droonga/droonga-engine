# -*- mode: ruby; coding: utf-8 -*-
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

Gem::Specification.new do |gem|
  gem.name          = "fluent-plugin-droonga"
  gem.version       = "0.0.2"
  gem.authors       = ["Droonga Project"]
  gem.email         = ["droonga@groonga.org"]
  gem.description   = "Droonga(distributed groonga) plugin for Fluent event collector"
  gem.summary       = gem.description
  gem.homepage      = "https://github.com/droonga/fluent-plugin-droonga"
  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]
  gem.add_dependency "fluentd"
  gem.add_dependency "rroonga", ">= 3.0.3"
  gem.add_dependency "groonga-command-parser"
  gem.add_dependency "fluent-logger"
  gem.add_dependency "serverengine"
  gem.add_development_dependency "rake"
  gem.add_development_dependency "bundler"
  gem.add_development_dependency "droonga-client"
  gem.add_development_dependency "test-unit"
  gem.add_development_dependency "test-unit-notify"
  gem.add_development_dependency "test-unit-rr"
end
