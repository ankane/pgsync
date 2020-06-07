require_relative "test_helper"

class VariableTest < Minitest::Test
  def setup
    truncate_tables ["posts"]
  end

  def test_variable
    source = 3.times.map { |i| {"id" => i + 1, "title" => "Post #{i + 1}"} }
    expected = [source[1]]

    insert($conn1, "posts", source)
    assert_works "variable:2", config: true
    assert_equal expected, $conn2.exec("SELECT * FROM posts ORDER BY 1, 2").to_a
  end

  def test_variable_id
    source = 3.times.map { |i| {"id" => i + 1, "title" => "Post #{i + 1}"} }
    expected = [source[1]]

    insert($conn1, "posts", source)
    assert_works "variable_id:2", config: true
    assert_equal expected, $conn2.exec("SELECT * FROM posts ORDER BY 1, 2").to_a
  end

  def test_variable_missing
    assert_error "Missing variables: 1", "variable", config: true
  end

  def test_variable_table
    assert_error "Cannot use parameters with tables", "posts:123", config: true
  end
end
