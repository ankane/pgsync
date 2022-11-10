require_relative "test_helper"

class DataSourceTest < Minitest::Test
  def test_no_source
    assert_error "No source", ""
  end

  def test_no_destination
    assert_error "No destination", "--from db1"
  end

  def test_source_command_error
    assert_error "Command exited with non-zero status:\nexit 1", "--config test/support/bad.yml"
  end

  def test_source_command_not_run_with_option
    assert_works "--config test/support/bad.yml --from pgsync_test1"
  end

  def test_database
    assert_prints "From: pgsync_test1\nTo: pgsync_test2", "--from pgsync_test1 --to pgsync_test2"
  end

  def test_url
    assert_prints "From: pgsync_test1 on localhost:5432\nTo: pgsync_test2 on localhost:5432", "--from postgres://localhost/pgsync_test1 --to postgres://localhost/pgsync_test2"
  end

  # def test_destination_danger
  #   assert_error "Danger! Add `to_safe: true` to `.pgsync.yml` if the destination is not localhost or 127.0.0.1", "--from pgsync_test1 --to postgres://hostname/db2"
  # end

  def test_nonexistent_source
    assert_error "FATAL:  database \"db1\" does not exist\n", "--from db1 --to pgsync_test2"
  end

  def test_nonexistent_destination
    assert_error "FATAL:  database \"db2\" does not exist\n", "--from pgsync_test1 --to db2"
  end
end
