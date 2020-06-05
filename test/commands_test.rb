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

  def test_bad_option
    assert_error "unknown option", "--bad"
  end

  def test_list
    output = assert_works "--list", dbs: true
    assert_match "public.posts", output
  end

  def test_config_and_db
    assert_error "Specify either --db or --config, not both", "--db test --config .pgsync.yml"
  end

  def test_config_not_found
    assert_error "Config file not found: bad.yml", "--config bad.yml"
  end

  def test_config_absolute_path
    path = File.expand_path("test/support/config.yml")
    assert_works "--config #{path}"
  end

  def test_db_not_found
    assert_error "Config file not found: .pgsync-bad.yml", "--db bad"
  end
end
