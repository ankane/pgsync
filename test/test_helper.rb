require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"
require "minitest/pride"
require "pg"
require "shellwords"
require "tmpdir"

$conn1 = PG::Connection.open(dbname: "pgsync_test1")
$conn1.exec("SET client_min_messages TO WARNING")
$conn1.exec(File.read("test/support/schema1.sql"))

$conn2 = PG::Connection.open(dbname: "pgsync_test2")
$conn2.exec("SET client_min_messages TO WARNING")
$conn2.exec(File.read("test/support/schema2.sql"))

class Minitest::Test
  def quietly
    if ENV["VERBOSE"]
      yield
    else
      capture_io do
        yield
      end
    end
  end

  def assert_works(args_str)
    quietly do
      PgSync::Client.new(Shellwords.split(args_str)).perform
    end
  end

  def assert_error(message, args_str)
    quietly do
      error = assert_raises { PgSync::Client.new(Shellwords.split(args_str)).perform }
      assert_equal message, error.message
    end
  end

  def assert_prints(message, args_str, debug: true)
    _, err = capture_io do
      args_str << " --debug" if debug
      PgSync::Client.new(Shellwords.split(args_str)).perform
    end
    assert_match message, err
  end
end
