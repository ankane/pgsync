require_relative "test_helper"

class PgSyncTest < Minitest::Test
  def test_no_source
    error = assert_raises(PgSync::Error) { PgSync::Client.new([]).perform }
    assert_equal "No source", error.message
  end

  def test_no_destination
    error = assert_raises(PgSync::Error) { PgSync::Client.new(["--from", "db1"]).perform }
    assert_equal "No destination", error.message
  end
end
