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

  def test_nonexistent_destination
    assert_error "FATAL:  database \"db2\" does not exist\n", "--from pgsync_db1 --to db2"
  end

  def test_version
    assert_output PgSync::VERSION, "-v"
  end

  def assert_error(message, args)
    error = nil
    capture_io do
      error = assert_raises(PgSync::Error) { PgSync::Client.new(Shellwords.split(args)).perform }
    end
    assert_equal message, error.message
  end

  def assert_output(message, args)
    _, err = capture_io do
      PgSync::Client.new(Shellwords.split("#{args} --debug")).perform
    end
    assert_equal "#{message}\n", err
  end
end
