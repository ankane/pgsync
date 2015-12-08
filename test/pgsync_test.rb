require_relative "test_helper"

class PgSyncTest < Minitest::Test
  def test_no_source
    client = PgSync::Client.new([])
    assert_raises(PgSync::Error, "No source") { client.perform }
  end
end
