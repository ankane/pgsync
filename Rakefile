require "bundler/gem_tasks"
require "rake/testtask"

task default: :test
Rake::TestTask.new do |t|
  t.libs << "test"
  t.pattern = "test/**/*_test.rb"
end

namespace :docker do
  task :build do
    require_relative "lib/pgsync/version"

    system "docker build --pull --no-cache --platform linux/amd64 -t ankane/pgsync:latest .", exception: true
    system "docker build --platform linux/amd64 -t ankane/pgsync:v#{PgSync::VERSION} .", exception: true
  end

  task :release do
    require_relative "lib/pgsync/version"

    system "docker push ankane/pgsync:latest", exception: true
    system "docker push ankane/pgsync:v#{PgSync::VERSION}", exception: true
  end
end
