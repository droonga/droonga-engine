# -*- encoding: utf-8 -*-
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
  gem.add_dependency "SocketIO"
end
