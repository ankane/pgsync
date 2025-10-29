require "bundler/gem_tasks"
require "rake/testtask"

Rake::TestTask.new do |t|
  t.pattern = "test/**/*_test.rb"
end

task default: :test

namespace :docker do
  task :build do
    require_relative "lib/pgsync/version"

    system "docker build --pull --no-cache -t ankane/pgsync:latest -t ankane/pgsync:v#{PgSync::VERSION} .", exception: true
  end

  task :release do
    require_relative "lib/pgsync/version"

    system "docker buildx build --push --pull --no-cache --platform linux/amd64,linux/arm64 -t ankane/pgsync:latest -t ankane/pgsync:v#{PgSync::VERSION} .", exception: true
  end
end
