require_relative "test_helper"
require "shellwords"

class PgSyncTest < Minitest::Test
  def test_no_source
    assert_error "No source", ""
  end

  def test_no_destination
    assert_error "No destination", "--from db1"
  end

  def test_source_command_error
    assert_error "Command exited with non-zero status:\nexit 1", "--from '$(exit 1)'"
  end

  def test_destination_danger
    assert_error "Danger! Add `to_safe: true` to `.pgsync.yml` if the destination is not localhost or 127.0.0.1", "--from db1 --to postgres://hostname/db2"
  end

  def test_nonexistent_source
    assert_error "FATAL:  database \"db1\" does not exist\n", "--from db1 --to db2"
  end

  def test_with_schema
    assert_prints "Completed in", "--from pgsync_db1?schema=sample_schema --to pgsync_db2?schema=sample_schema"
  end

  def test_nonexistent_destination
    assert_error "FATAL:  database \"db2\" does not exist\n", "--from pgsync_db1 --to db2"
  end

  def test_missing_column
    assert_prints "Missing columns: zip_code", "--from pgsync_db1 --to pgsync_db2"
  end

  def test_extra_column
    assert_prints "Extra columns: zip_code", "--from pgsync_db2 --to pgsync_db1"
  end

  def test_parallel
    assert_prints "Completed in", "--from pgsync_db1 --to pgsync_db2", debug: false
  end

  def test_version
    assert_prints PgSync::VERSION, "-v"
  end

  def assert_error(message, args_str)
    error = nil
    capture_io do
      error = assert_raises(PgSync::Error) { PgSync::Client.new(Shellwords.split(args_str)).perform }
    end
    assert_equal message, error.message
  end

  def assert_prints(message, args_str, debug: true)
    _, err = capture_io do
      args_str << " --debug" if debug
      PgSync::Client.new(Shellwords.split(args_str)).perform
    end
    assert_match message, err
  end
end
