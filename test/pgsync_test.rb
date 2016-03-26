require_relative "test_helper"

class PgSyncTest < Minitest::Test
  def test_no_config
    assert_raises(PgSync::Error, "Config not found") { PgSync::Client.new([]) }
  end
end
