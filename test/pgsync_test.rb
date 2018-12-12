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

  # def test_destination_danger
  #   assert_error "Danger! Add `to_safe: true` to `.pgsync.yml` if the destination is not localhost or 127.0.0.1", "--from pgsync_test1 --to postgres://hostname/db2"
  # end

  def test_nonexistent_source
    assert_error "FATAL:  database \"db1\" does not exist\n", "--from db1 --to db2"
  end

  def test_nonexistent_destination
    assert_error "FATAL:  database \"db2\" does not exist\n", "--from pgsync_test1 --to db2"
  end

  def test_missing_column
    assert_prints "Missing columns: zip_code", "--from pgsync_test1 --to pgsync_test2"
  end

  def test_extra_column
    assert_prints "Extra columns: zip_code", "--from pgsync_test2 --to pgsync_test1"
  end

  def test_overwrite
    assert_works "--from pgsync_test2 --to pgsync_test1 --overwrite"
  end

  def test_table
    assert_works "Users --from pgsync_test2 --to pgsync_test1"
  end

  def test_partial
    assert_works "Users 'WHERE \"Id\" > 100' --from pgsync_test2 --to pgsync_test1"
  end

  def test_group
    assert_works "group1 --from pgsync_test2 --to pgsync_test1 --config test/support/config.yml"
  end

  def test_data_rules
    conn1 = PG::Connection.open(dbname: "pgsync_test1")
    2.times do
      conn1.exec("INSERT INTO \"Users\" (email, phone, token, attempts, created_on, updated_at, ip, name, nonsense, untouchable)
      VALUES ('hi@example.org', '555-555-5555', 'token123', 1, NOW(), NOW(), '127.0.0.1', 'Hi', 'Text', 'rock');")
    end
    assert_works "Users --from pgsync_test1 --to pgsync_test2 --config test/support/config.yml"
    conn2 = PG::Connection.open(dbname: "pgsync_test2")
    result = conn2.exec("SELECT * FROM \"Users\"").to_a
    row = result.first
    assert_equal "email#{row["Id"]}@example.org", row["email"]
    assert_equal "secret#{row["Id"]}", row["token"]
  end

  def test_parallel
    assert_prints "Completed in", "--from pgsync_test1 --to pgsync_test2", debug: false
  end

  def test_version
    assert_prints PgSync::VERSION, "-v"
  end

  def assert_works(args_str)
    capture_io do
      assert PgSync::Client.new(Shellwords.split(args_str)).perform
    end
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
