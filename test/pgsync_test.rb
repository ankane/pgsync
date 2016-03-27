require_relative "test_helper"

class PgSyncTest < Minitest::Test
  def test_no_config
    assert_raises(PgSync::Error, "Config not found") { perform("", config: false) }
  end

  def test_config
    assert perform("")
  end

  def perform(command, config: true)
    command << " --config test/support/config.yml" if config
    client = PgSync::Client.new(command.split(" "))
    client.perform
  end
end
