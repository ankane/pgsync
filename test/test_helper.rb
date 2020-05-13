require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"
require "minitest/pride"
require "pg"
require "shellwords"
require "tmpdir"
require "open3"

def connect(dbname)
  conn = PG::Connection.open(dbname: dbname)
  conn.exec("SET client_min_messages TO WARNING")
  conn.type_map_for_results = PG::BasicTypeMapForResults.new(conn)
  conn.exec(File.read("test/support/schema#{dbname[-1]}.sql"))
  conn
end

$conn1 = connect("pgsync_test1")
$conn2 = connect("pgsync_test2")
$conn3 = connect("pgsync_test3")

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
