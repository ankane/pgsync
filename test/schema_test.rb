require_relative "test_helper"

class SchemaTest < Minitest::Test
  def setup
    $conn3.exec(File.read("test/support/schema3.sql"))
    truncate($conn1, "posts")
  end

  def test_schema_only
    insert($conn1, "posts", [{"id" => 1}])
    assert_equal [], tables($conn3)
    assert_works "--from pgsync_test1 --to pgsync_test3 --schema-only --all-schemas"
    assert_equal ["other.pets", "public.Users", "public.comments", "public.comments2", "public.posts", "public.robots"], tables($conn3)
    assert_equal [], $conn3.exec("SELECT * FROM posts").to_a
  end

  def test_schema_first
    insert($conn1, "posts", [{"id" => 1}])
    assert_equal [], tables($conn3)
    assert_works "--from pgsync_test1 --to pgsync_test3 --schema-first --all-schemas"
    assert_equal ["other.pets", "public.Users", "public.comments", "public.comments2", "public.posts", "public.robots"], tables($conn3)
    assert_equal [{"id" => 1}], $conn3.exec("SELECT id FROM posts").to_a
  end

  def tables(conn)
    # sort in Ruby, as Postgres can return different order on different platforms
    conn.exec("SELECT table_schema || '.' || table_name AS table FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')").map { |v| v["table"] }.sort
  end
end
