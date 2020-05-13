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
$conn1.type_map_for_results = PG::BasicTypeMapForResults.new($conn1)

$conn2 = PG::Connection.open(dbname: "pgsync_test2")
$conn2.exec("SET client_min_messages TO WARNING")
$conn2.exec(File.read("test/support/schema2.sql"))
$conn2.type_map_for_results = PG::BasicTypeMapForResults.new($conn2)

class Minitest::Test
  def verbose?
    ENV["VERBOSE"]
  end

  def run_command(command, dbs: false)
    command << " --from pgsync_test1 --to pgsync_test2" if dbs
    if verbose?
      puts
      puts "$ pgsync #{command}"
    end
    exe = File.expand_path("../exe/pgsync", __dir__)
    output, status = Open3.capture2e(exe, *Shellwords.split(command))
    puts output if verbose?
    [output, status]
  end

  def assert_works(command, dbs: false)
    output, status = run_command(command, dbs: dbs)
    assert status.success?
    output
  end

  def assert_error(message, command)
    output, status = run_command(command)
    assert !status.success?
    assert_match message, output
  end

  def assert_prints(message, command)
    output, status = run_command(command)
    assert_match message, output
  end
end
