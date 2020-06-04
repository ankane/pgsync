require_relative "test_helper"

class CommandsTest < Minitest::Test
  def test_help
    assert_prints "Usage:", "-h"
    assert_prints "Usage:", "--help"
  end

  def test_version
    assert_prints PgSync::VERSION, "-v"
    assert_prints PgSync::VERSION, "--version"
  end

  def test_list
    output = assert_works "--list", dbs: true
    assert_match "public.posts", output
  end

  def test_config_and_db
    skip "TODO uncomment for 0.6.0"
    assert_error "Specify either --db or --config, not both", "--db test --config .pgsync.yml"
  end

  def test_config_not_found
    skip "TODO uncomment for 0.6.0"
    assert_error "Config file not found", "--config bad.yml"
  end

  def test_config_absolute_path
    path = File.expand_path("test/support/config.yml")
    assert_works "--config #{path}"
  end
end
