require_relative "lib/pgsync/version"

Gem::Specification.new do |spec|
  spec.name          = "pgsync"
  spec.version       = PgSync::VERSION
  spec.summary       = "Sync Postgres data between databases"
  spec.homepage      = "https://github.com/ankane/pgsync"
  spec.license       = "MIT"

  spec.authors       = "Andrew Kane"
  spec.email         = "andrew@ankane.org"

  spec.files         = Dir["*.{md,txt}", "{lib,exe}/**/*", "config.yml"]
  spec.require_path  = "lib"

  spec.bindir        = "exe"
  spec.executables   = ["pgsync"]

  spec.required_ruby_version = ">= 2.5"

  spec.add_dependency "parallel"
  spec.add_dependency "pg", ">= 0.18.2"
  spec.add_dependency "slop", ">= 4.10.1"
  spec.add_dependency "tty-spinner"
end
