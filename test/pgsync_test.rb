require_relative "test_helper"

class PgSyncTest < Minitest::Test
  def test_no_config
    error = assert_raises(PgSync::Error) { PgSync::Client.new([]).perform }
    assert_equal "No source", error.message
  end
end
