require_relative "test_helper"

class SyncTest < Minitest::Test
  def setup
    [$conn1, $conn2].each do |conn|
      %w(Users posts comments robots).each do |table|
        conn.exec("TRUNCATE #{quote_ident(table)} CASCADE")
      end
    end
  end

  def test_truncate
    source = 3.times.map { |i| {"id" => i + 1, "title" => "Post #{i + 1}"} }
    dest = [{"id" => 1, "title" => "First Post"}, {"id" => 4, "title" => "Post 4"}]
    expected = source
    assert_result("", source, dest, expected)
  end

  def test_overwrite
    source = 3.times.map { |i| {"id" => i + 1, "title" => "Post #{i + 1}"} }
    dest = [{"id" => 1, "title" => "First Post"}, {"id" => 4, "title" => "Post 4"}]
    expected = source + [dest[1]]
    assert_result("--overwrite", source, dest, expected)
  end

  def test_preserve
    source = 3.times.map { |i| {"id" => i + 1, "title" => "Post #{i + 1}"} }
    dest = [{"id" => 1, "title" => "First Post"}, {"id" => 4, "title" => "Post 4"}]
    expected = [dest[0]] + source[1..-1] + [dest[1]]
    assert_result("--preserve", source, dest, expected)
  end

  def test_where
    source = 3.times.map { |i| {"id" => i + 1, "title" => "Post #{i + 1}"} }
    dest = []
    expected = [source[0]]
    assert_result(" 'WHERE id = 1'", source, dest, expected)
  end

  def test_no_source
    assert_error "No source", ""
  end

  def test_no_destination
    assert_error "No destination", "--from db1"
  end

  def test_source_command_error
    assert_error "Command exited with non-zero status:\nexit 1", "--from '$(exit 1)'"
  end

  # def test_destination_danger
  #   assert_error "Danger! Add `to_safe: true` to `.pgsync.yml` if the destination is not localhost or 127.0.0.1", "--from pgsync_test1 --to postgres://hostname/db2"
  # end

  def test_nonexistent_source
    assert_error "FATAL:  database \"db1\" does not exist\n", "--from db1 --to pgsync_test2"
  end

  def test_nonexistent_destination
    assert_error "FATAL:  database \"db2\" does not exist\n", "--from pgsync_test1 --to db2"
  end

  def test_missing_column
    assert_prints "Missing columns: zip_code", "--from pgsync_test1 --to pgsync_test2"
  end

  def test_extra_column
    assert_prints "Extra columns: zip_code", "--from pgsync_test2 --to pgsync_test1"
  end

  def test_table
    assert_works "Users --from pgsync_test2 --to pgsync_test1"
  end

  def test_table_unknown
    assert_error "Table does not exist in source: bad", "bad --from pgsync_test2 --to pgsync_test1"
  end

  def test_partial
    assert_works "Users 'WHERE \"Id\" > 100' --from pgsync_test2 --to pgsync_test1"
  end

  def test_group
    assert_works "group1 --from pgsync_test2 --to pgsync_test1 --config test/support/config.yml"
  end

  def test_group_unknown
    assert_error "Group not found: bad", "--from pgsync_test2 --to pgsync_test1 --groups bad"
  end

  def test_data_rules
    2.times do
      insert($conn1, "Users", [{
        "email" => "hi@example.org",
        "phone" => "555-555-5555",
        "token" => "token123",
        "attempts" => 1,
        "created_on" => Date.today,
        "updated_at" => Time.now,
        "ip" => "127.0.0.1",
        "name" => "Hi",
        "nonsense" => "Text",
        "untouchable" => "rock"
      }])
    end
    assert_works "Users --from pgsync_test1 --to pgsync_test2 --config test/support/config.yml"
    result = $conn2.exec("SELECT * FROM \"Users\"").to_a
    row = result.first
    assert_equal "email#{row["Id"]}@example.org", row["email"]
    assert_equal "secret#{row["Id"]}", row["token"]
    assert_equal "rock", row["untouchable"]
  end

  def test_schema_only
    insert($conn1, "posts", [{"id" => 1}])
    recreate_schema
    assert_equal [], tables($conn3)
    assert_works "--from pgsync_test1 --to pgsync_test3 --schema-only --all-schemas"
    assert_equal ["other.pets", "public.Users", "public.comments", "public.posts", "public.robots"], tables($conn3)
    assert_equal [], $conn3.exec("SELECT * FROM posts").to_a
  end

  def test_schema_first
    insert($conn1, "posts", [{"id" => 1}])
    recreate_schema
    assert_equal [], tables($conn3)
    assert_works "--from pgsync_test1 --to pgsync_test3 --schema-first --all-schemas"
    assert_equal ["other.pets", "public.Users", "public.comments", "public.posts", "public.robots"], tables($conn3)
    assert_equal [{"id" => 1}], $conn3.exec("SELECT id FROM posts").to_a
  end

  def test_defer_constraints
    insert($conn1, "posts", [{"id" => 1}])
    insert($conn1, "comments", [{"post_id" => 1}])
    assert_error "Sync failed for 1 table: comments", "comments,posts --from pgsync_test1 --to pgsync_test2 --debug"
    assert_works "comments,posts --from pgsync_test1 --to pgsync_test2 --defer-constraints"
    assert_works "comments,posts --from pgsync_test1 --to pgsync_test2 --defer-constraints --overwrite"
    assert_works "comments,posts --from pgsync_test1 --to pgsync_test2 --defer-constraints --preserve"
    assert_equal [{"id" => 1}], $conn2.exec("SELECT id FROM posts ORDER BY id").to_a
    assert_equal [{"post_id" => 1}], $conn2.exec("SELECT post_id FROM comments ORDER BY post_id").to_a
  end

  def test_disable_user_triggers
    insert($conn1, "robots", [{"name" => "Test"}])
    assert_error "Sync failed for 1 table: robots", "robots --from pgsync_test1 --to pgsync_test2"
    assert_works "robots --from pgsync_test1 --to pgsync_test2 --disable-user-triggers"
    assert_equal [{"name" => "Test"}], $conn2.exec("SELECT name FROM robots ORDER BY id").to_a
  end

  def test_disable_integrity
    insert($conn1, "posts", [{"id" => 1}])
    insert($conn1, "comments", [{"post_id" => 1}])
    assert_error "Sync failed for 1 table: comments", "comments --from pgsync_test1 --to pgsync_test2"
    assert_works "comments --from pgsync_test1 --to pgsync_test2 --disable-integrity"
    # integrity is lost! (as expected)
    assert_equal [], $conn2.exec("SELECT * FROM posts ORDER BY id").to_a
    assert_equal [{"post_id" => 1}], $conn2.exec("SELECT post_id FROM comments ORDER BY post_id").to_a
  end

  def assert_result(command, source, dest, expected)
    insert($conn1, "posts", source)
    insert($conn2, "posts", dest)

    assert_equal source, $conn1.exec("SELECT * FROM posts ORDER BY id").to_a
    assert_equal dest, $conn2.exec("SELECT * FROM posts ORDER BY id").to_a

    assert_works "posts #{command}", dbs: true

    assert_equal source, $conn1.exec("SELECT * FROM posts ORDER BY id").to_a
    assert_equal expected, $conn2.exec("SELECT * FROM posts ORDER BY id").to_a
  end

  def insert(conn, table, rows)
    return if rows.empty?

    keys = rows.flat_map { |r| r.keys }.uniq
    values = rows.map { |r| keys.map { |k| r[k] } }

    key_str = keys.map { |k| quote_ident(k) }.join(", ")
    params_str = values.size.times.map { |i| "(" + keys.size.times.map { |j| "$#{i * keys.size + j + 1}" }.join(", ") + ")" }.join(", ")
    insert_str = "INSERT INTO #{quote_ident(table)} (#{key_str}) VALUES #{params_str}"
    conn.exec_params(insert_str, values.flatten)
  end

  def recreate_schema
    $conn3.exec(File.read("test/support/schema3.sql"))
  end

  def tables(conn)
    conn.exec("SELECT table_schema || '.' || table_name AS table FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog') ORDER BY 1").map { |v| v["table"] }
  end

  def quote_ident(ident)
    PG::Connection.quote_ident(ident)
  end
end
