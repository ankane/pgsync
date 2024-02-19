require_relative "test_helper"

class SyncTest < Minitest::Test
  def setup
    truncate_tables ["posts", "comments", "books", "robots"]
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

  def test_overwrite_skips_identical_rows
    source = 3.times.map { |i| {"id" => i + 1, "title" => "Post #{i + 1}"} }
    dest = source
    expected = source
    assert_result("--overwrite", source, dest, expected)
    # get ctids (~ table row versions)
    ctids = conn2.exec("select ctid from posts").to_a
    # rerun the overwrite
    assert_works "posts --overwrite", config: true
    ctids2 = conn2.exec("select ctid from posts").to_a
    # verify ctids have not changed
    assert_equal(ctids, ctids2)
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

  def test_generated
    skip if server_version_num < 120000

    [conn1, conn2].each do |conn|
      conn.exec("DROP TABLE IF EXISTS shares")
      conn.exec <<~EOS
        CREATE TABLE shares (
          id SERIAL PRIMARY KEY,
          gen integer GENERATED ALWAYS AS (id + 1) STORED
        );
      EOS
    end

    source = 3.times.map { |i| {"id" => i + 1, "gen" => i + 2} }
    dest = []
    expected = source
    assert_result("", source, dest, expected, "shares")

    truncate_tables ["shares"]
    assert_result("--overwrite", source, dest, expected, "shares")

    truncate_tables ["shares"]
    assert_result("--preserve", source, dest, expected, "shares")
  end

  def test_overwrite_no_primary_key
    assert_error "chapters (Primary key required for --overwrite)", "chapters --overwrite", config: true
  end

  def test_preserve_no_primary_key
    assert_error "chapters (Primary key required for --preserve)", "chapters --preserve", config: true
  end

  def test_no_shared_fields
    assert_prints "authors: No fields to copy", "authors", config: true
  end

  def test_missing_column
    assert_prints "Missing columns: current_mood, zip_code", "Users", config: true
  end

  def test_extra_column
    assert_prints "Extra columns: current_mood, zip_code", "Users --from pgsync_test2 --to pgsync_test1"
  end

  def test_different_column_types
    assert_prints "Different column types: pages (integer -> bigint)", "chapters", config: true
  end

  def test_notice
    skip if ENV["TRAVIS"]
    assert_prints "NOTICE:  truncate cascades to table \"comments\"", "posts", config: true
  end

  def test_defer_constraints_v1
    insert(conn1, "posts", [{"id" => 1}])
    insert(conn1, "comments", [{"post_id" => 1}])
    assert_error "Sync failed for 1 table: comments", "comments,posts --jobs 1", config: true
    assert_works "comments,posts --defer-constraints-v1", config: true
    assert_works "comments,posts --defer-constraints-v1 --overwrite", config: true
    assert_works "comments,posts --defer-constraints-v1 --preserve", config: true
    assert_equal [{"id" => 1}], conn2.exec("SELECT id FROM posts ORDER BY id").to_a
    assert_equal [{"post_id" => 1}], conn2.exec("SELECT post_id FROM comments ORDER BY post_id").to_a
  end

  def test_defer_constraints_v1_not_deferrable
    insert(conn1, "posts", [{"id" => 1}])
    insert(conn1, "comments2", [{"post_id" => 1}])
    assert_prints "Non-deferrable constraints: comments2_post_id_fkey", "comments2,posts --defer-constraints-v1", config: true
    assert_error "violates foreign key constraint", "comments2,posts --defer-constraints-v1", config: true
  end

  def test_defer_constraints
    insert(conn1, "posts", [{"id" => 1}])
    insert(conn1, "comments", [{"post_id" => 1}])
    assert_error "Sync failed for 1 table: comments", "comments,posts --jobs 1", config: true
    assert_works "comments,posts --defer-constraints", config: true
    assert_works "comments,posts --defer-constraints --overwrite", config: true
    assert_works "comments,posts --defer-constraints --preserve", config: true
    assert_equal [{"id" => 1}], conn2.exec("SELECT id FROM posts ORDER BY id").to_a
    assert_equal [{"post_id" => 1}], conn2.exec("SELECT post_id FROM comments ORDER BY post_id").to_a
  end

  def test_defer_constraints_not_deferrable
    insert(conn1, "posts", [{"id" => 1}])
    insert(conn1, "comments2", [{"post_id" => 1}])
    assert_error "Sync failed for 1 table: comments2", "comments2,posts --jobs 1", config: true
    assert_works "comments2,posts --defer-constraints", config: true
    assert_works "comments2,posts --defer-constraints --overwrite", config: true
    assert_works "comments2,posts --defer-constraints --preserve", config: true
    assert_equal [{"id" => 1}], conn2.exec("SELECT id FROM posts ORDER BY id").to_a
    assert_equal [{"post_id" => 1}], conn2.exec("SELECT post_id FROM comments2 ORDER BY post_id").to_a
  end

  def test_disable_user_triggers
    insert(conn1, "robots", [{"name" => "Test"}])
    assert_error "Sync failed for 1 table: robots", "robots", config: true
    assert_works "robots --disable-user-triggers", config: true
    assert_equal [{"name" => "Test"}], conn2.exec("SELECT name FROM robots ORDER BY id").to_a
  end

  def test_disable_integrity
    insert(conn1, "posts", [{"id" => 1}])
    insert(conn1, "comments", [{"post_id" => 1}])
    assert_error "Sync failed for 1 table: comments", "comments", config: true
    assert_works "comments --disable-integrity", config: true
    # integrity is lost! (as expected)
    assert_equal [], conn2.exec("SELECT * FROM posts ORDER BY id").to_a
    assert_equal [{"post_id" => 1}], conn2.exec("SELECT post_id FROM comments ORDER BY post_id").to_a
  end

  def test_disable_integrity_v2
    insert(conn1, "posts", [{"id" => 1}])
    insert(conn1, "comments", [{"post_id" => 1}])
    assert_error "Sync failed for 1 table: comments", "comments", config: true
    assert_works "comments --disable-integrity-v2", config: true
    # integrity is lost! (as expected)
    assert_equal [], conn2.exec("SELECT * FROM posts ORDER BY id").to_a
    assert_equal [{"post_id" => 1}], conn2.exec("SELECT post_id FROM comments ORDER BY post_id").to_a
  end
end
