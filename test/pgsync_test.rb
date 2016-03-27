require_relative "test_helper"

class PgSyncTest < Minitest::Test
  def test_no_config
    assert_raises(PgSync::Error) { PgSync::Client.new([]).perform }
  end
end
