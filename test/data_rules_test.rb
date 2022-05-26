require_relative "test_helper"

class DataRulesTest < Minitest::Test
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
        "untouchable" => "rock"
      }])
    end
  end

  def test_rules
    assert_works "Users", config: true
    result = conn2.exec("SELECT * FROM \"Users\"").to_a
    row = result.first
    assert_equal "email#{row["Id"]}@example.org", row["email"]
    assert_equal "secret#{row["Id"]}", row["token"]
    assert row["ip"].end_with?("0.0.1")
    assert_equal 1, row["name"].size
    assert_equal "rock", row["untouchable"]
    assert_equal "shell", row["nonsense"]
  end

  def test_no_rules
    assert_works "Users --no-rules", config: true
    result = conn2.exec("SELECT * FROM \"Users\"").to_a
    row = result.first
    assert_equal "hi@example.org", row["email"]
    assert_equal "555-555-5555", row["phone"]
    assert_equal "token123", row["token"]
    assert_equal 1, row["attempts"]
    assert_equal "1.1.1.1", row["ip"]
    assert_equal "Hi", row["name"]
    assert_equal "Text", row["nonsense"]
    assert_equal "rock", row["untouchable"]
  end
end
