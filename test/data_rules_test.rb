require_relative "test_helper"

class DataRulesTest < Minitest::Test
  def setup
    truncate_tables ["Users"]
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
    assert_works "Users", config: true
    result = $conn2.exec("SELECT * FROM \"Users\"").to_a
    row = result.first
    assert_equal "email#{row["Id"]}@example.org", row["email"]
    assert_equal "secret#{row["Id"]}", row["token"]
    assert_equal "rock", row["untouchable"]
  end
end
