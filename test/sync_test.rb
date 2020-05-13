require_relative "test_helper"

class SyncTest < Minitest::Test
  def setup
    [$conn1, $conn2].each do |conn|
      conn.exec("TRUNCATE \"Users\"")
      conn.exec("TRUNCATE posts CASCADE")
      conn.exec("TRUNCATE comments CASCADE")
      conn.exec("TRUNCATE robots")
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
      $conn1.exec("INSERT INTO \"Users\" (email, phone, token, attempts, created_on, updated_at, ip, name, nonsense, untouchable)
      VALUES ('hi@example.org', '555-555-5555', 'token123', 1, NOW(), NOW(), '127.0.0.1', 'Hi', 'Text', 'rock');")
    end
    assert_works "Users --from pgsync_test1 --to pgsync_test2 --config test/support/config.yml"
    result = $conn2.exec("SELECT * FROM \"Users\"").to_a
    row = result.first
    assert_equal "email#{row["Id"]}@example.org", row["email"]
    assert_equal "secret#{row["Id"]}", row["token"]
  end

  def test_schema_only
    assert_works "--from pgsync_test1 --to pgsync_test3 --schema-only --all-schemas"
  end

  def test_schema_first
    assert_works "--from pgsync_test1 --to pgsync_test3 --schema-first --all-schemas"
  end

  def test_defer_constraints
    $conn1.exec("INSERT INTO posts (id) VALUES (1)")
    $conn1.exec("INSERT INTO comments (post_id) VALUES (1)")
    assert_error "Sync failed for 1 table: comments", "comments,posts --from pgsync_test1 --to pgsync_test2 --debug"
    assert_works "comments,posts --from pgsync_test1 --to pgsync_test2 --defer-constraints"
    assert_works "comments,posts --from pgsync_test1 --to pgsync_test2 --defer-constraints --overwrite"
    assert_works "comments,posts --from pgsync_test1 --to pgsync_test2 --defer-constraints --preserve"
  end

  def test_disable_user_triggers
    $conn1.exec("INSERT INTO robots (name) VALUES ('Test')")
    assert_error "Sync failed for 1 table: robots", "robots --from pgsync_test1 --to pgsync_test2"
    assert_works "robots --from pgsync_test1 --to pgsync_test2 --disable-user-triggers"
  end

  def test_disable_integrity
    $conn1.exec("INSERT INTO posts (id) VALUES (1)")
    $conn1.exec("INSERT INTO comments (post_id) VALUES (1)")
    assert_error "Sync failed for 1 table: comments", "comments --from pgsync_test1 --to pgsync_test2"
    assert_works "comments --from pgsync_test1 --to pgsync_test2 --disable-integrity"
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
    params_str = values.size.times.map { |i| "(" + keys.size.times.map { |j| "$#{i * keys.size + j + 1}" }.join(", ") + ")" }.join(", ")
    # don't worry about escaping columns for tests
    conn.exec_params("INSERT INTO #{table} (#{keys.join(", ")}) VALUES #{params_str}", values.flatten)
  end
end
