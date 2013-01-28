# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Kotoumi project
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
  gem.name          = "fluent-plugin-kotoumi"
  gem.version       = "0.0.1"
  gem.authors       = ["Kotoumi project"]
  gem.email         = ["kotoumi@groonga.org"]
  gem.description   = "kotoumi(distributed groonga) plugin for Fluent event collector"
  gem.summary       = gem.description
  gem.homepage      = "https://github.com/groonga/fluent-plugin-kotoumi"
  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]
  gem.add_dependency "fluentd"
  gem.add_dependency "groonga"
  gem.add_dependency "socketio-client"
end
