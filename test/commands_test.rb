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
end
