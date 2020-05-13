require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"
require "minitest/pride"
require "pg"
require "shellwords"
require "tmpdir"
require "open3"

$conn1 = PG::Connection.open(dbname: "pgsync_test1")
$conn1.exec("SET client_min_messages TO WARNING")
$conn1.exec(File.read("test/support/schema1.sql"))

$conn2 = PG::Connection.open(dbname: "pgsync_test2")
$conn2.exec("SET client_min_messages TO WARNING")
$conn2.exec(File.read("test/support/schema2.sql"))

class Minitest::Test
  def run_command(args_str)
    exe = File.expand_path("../exe/pgsync", __dir__)
    output, status = Open3.capture2e(exe, *Shellwords.split(args_str))
    puts output if ENV["VERBOSE"]
    [output, status]
  end

  def assert_works(args_str)
    output, status = run_command(args_str)
    assert status.success?
  end

  def assert_error(message, args_str)
    output, status = run_command(args_str)
    assert !status.success?
    assert_match message, output
  end

  def assert_prints(message, args_str, debug: true)
    args_str << " --debug" if debug
    output, status = run_command(args_str)
    assert_match message, output
  end
end
