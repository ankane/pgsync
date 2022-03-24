require_relative "test_helper"

class SkipUpdateOnConflict < Minitest::Test
  def setup
    truncate_tables ["Users"]

    2.times do
      insert(conn1, "Users", [{
        "email" => "hi@example.org",
        "phone" => "555-555-5555",
        "token" => "token123",
        "attempts" => 1,
        "created_on" => Date.today,
        "updated_at" => Time.now,
        "ip" => "1.1.1.1",
        "name" => "Hi",
        "nonsense" => "Text",
        "untouchable" => "rock",
        "env_token" => "prod",
      }])
    end
  end

  def test_set_new_rows_null
    assert_works "Users --overwrite", config: true
    result = conn2.exec("SELECT * FROM \"Users\"").to_a
    row = result.first
    assert_equal nil, row["env_token"]  # This value should be discarded
  end

  def test_preserve_old_value_on_update
    # User #1 exists on destination database with slightly different data
    insert(conn2, "Users", [{
      "email" => "hi@example.org",
      "phone" => "555-555-5555",
      "token" => "token123",
      "attempts" => 1,
      "created_on" => Date.today,
      "updated_at" => Time.now,
      "ip" => "1.1.1.1",
      "name" => "Hi",
      "nonsense" => "Text",
      "untouchable" => "paper",
      "env_token" => "beta", # This value should be preserved
    }])

    assert_works "Users --overwrite", config: true
    result = conn2.exec("SELECT * FROM \"Users\"").to_a
    row = result.first
    assert_equal "beta", row["env_token"]
  end
end
