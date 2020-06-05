require_relative "test_helper"

class SyncTest < Minitest::Test
  def setup
    [$conn1, $conn2].each do |conn|
      %w(Users posts comments books robots).each do |table|
        truncate(conn, table)
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

  def test_overwrite_multicolumn_primary_key
    source = [
      {"id" => 1, "id2" => 1, "title" => "Post 1"},
      {"id" => 1, "id2" => 2, "title" => "Post 2"},
      {"id" => 1, "id2" => 3, "title" => "Post 3"},
    ]
    dest = [{"id" => 1, "id2" => 1, "title" => "First Post"}, {"id" => 1, "id2" => 4, "title" => "Post 4"}]
    expected = source + [dest[1]]
    assert_result("--overwrite", source, dest, expected, "books")
  end

  def test_preserve_multicolumn_primary_key
    source = [
      {"id" => 1, "id2" => 1, "title" => "Post 1"},
      {"id" => 1, "id2" => 2, "title" => "Post 2"},
      {"id" => 2, "id2" => 4, "title" => "Post 3"},
    ]
    dest = [{"id" => 1, "id2" => 1, "title" => "First Post"}, {"id" => 3, "id2" => 4, "title" => "Post 4"}]
    expected = [dest[0]] + source[1..-1] + [dest[1]]
    assert_result("--preserve", source, dest, expected, "books")
  end

  def test_all
    assert_works "", dbs: true
  end

  def test_no_shared_fields
    assert_prints "authors: No fields to copy", "authors", dbs: true
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
    assert_error "Command exited with non-zero status:\nexit 1", "--config test/support/bad.yml"
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
    assert_prints "Missing columns: current_mood, zip_code", "Users", dbs: true
  end

  def test_extra_column
    assert_prints "Extra columns: current_mood, zip_code", "Users --from pgsync_test2 --to pgsync_test1"
  end

  def test_different_column_types
    assert_prints "Different column types: pages (integer -> bigint)", "chapters", dbs: true
  end

  def test_table_unknown
    assert_error "Table not found in source: bad", "bad", dbs: true
  end

  def test_group
    assert_works "group1 --config test/support/config.yml", dbs: true
  end

  def test_group_unknown
    assert_error "Group not found: bad", "--groups bad", dbs: true
  end

  def test_in_batches
    source = 3.times.map { |i| {"id" => i + 1, "title" => "Post #{i + 1}"} }
    dest = []
    expected = source
    assert_result("--in-batches --batch-size 1", source, dest, expected)
  end

  def test_in_batches_existing_data
    source = 3.times.map { |i| {"id" => i + 1, "title" => "Post #{i + 1}"} }
    dest = [{"id" => 1, "title" => "First Post"}, {"id" => 4, "title" => "Post 4"}]
    expected = dest
    assert_result("--in-batches --batch-size 1", source, dest, expected)
  end

  def test_in_batches_overwrite
    assert_error "Cannot use --overwrite with --in-batches", "posts --in-batches --overwrite", dbs: true
  end

  def test_in_batches_multiple_tables
    assert_error "Cannot use --in-batches with multiple tables", "--in-batches", dbs: true
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
    assert_works "Users --config test/support/config.yml", dbs: true
    result = $conn2.exec("SELECT * FROM \"Users\"").to_a
    row = result.first
    assert_equal "email#{row["Id"]}@example.org", row["email"]
    assert_equal "secret#{row["Id"]}", row["token"]
    assert_equal "rock", row["untouchable"]
  end

  def test_defer_constraints
    insert($conn1, "posts", [{"id" => 1}])
    insert($conn1, "comments", [{"post_id" => 1}])
    assert_error "Sync failed for 1 table: comments", "comments,posts --jobs 1", dbs: true
    assert_works "comments,posts --defer-constraints", dbs: true
    assert_works "comments,posts --defer-constraints --overwrite", dbs: true
    assert_works "comments,posts --defer-constraints --preserve", dbs: true
    assert_equal [{"id" => 1}], $conn2.exec("SELECT id FROM posts ORDER BY id").to_a
    assert_equal [{"post_id" => 1}], $conn2.exec("SELECT post_id FROM comments ORDER BY post_id").to_a
  end

  def test_defer_constraints_not_deferrable
    insert($conn1, "posts", [{"id" => 1}])
    insert($conn1, "comments2", [{"post_id" => 1}])
    assert_error "violates foreign key constraint", "comments2,posts --defer-constraints", dbs: true
  end

  def test_disable_user_triggers
    insert($conn1, "robots", [{"name" => "Test"}])
    assert_error "Sync failed for 1 table: robots", "robots", dbs: true
    assert_works "robots --disable-user-triggers", dbs: true
    assert_equal [{"name" => "Test"}], $conn2.exec("SELECT name FROM robots ORDER BY id").to_a
  end

  def test_disable_integrity
    insert($conn1, "posts", [{"id" => 1}])
    insert($conn1, "comments", [{"post_id" => 1}])
    assert_error "Sync failed for 1 table: comments", "comments", dbs: true
    assert_works "comments --disable-integrity", dbs: true
    # integrity is lost! (as expected)
    assert_equal [], $conn2.exec("SELECT * FROM posts ORDER BY id").to_a
    assert_equal [{"post_id" => 1}], $conn2.exec("SELECT post_id FROM comments ORDER BY post_id").to_a
  end

  def assert_result(command, source, dest, expected, table = "posts")
    insert($conn1, table, source)
    insert($conn2, table, dest)

    assert_equal source, $conn1.exec("SELECT * FROM #{table} ORDER BY 1, 2").to_a
    assert_equal dest, $conn2.exec("SELECT * FROM #{table} ORDER BY 1, 2").to_a

    assert_works "#{table} #{command}", dbs: true

    assert_equal source, $conn1.exec("SELECT * FROM #{table} ORDER BY 1, 2").to_a
    assert_equal expected, $conn2.exec("SELECT * FROM #{table} ORDER BY 1, 2").to_a
  end
end
