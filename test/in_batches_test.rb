require_relative "test_helper"

class InBatchesTest < Minitest::Test
  def setup
    truncate_tables ["posts"]
  end

  def test_works
    source = 3.times.map { |i| {"id" => i + 1, "title" => "Post #{i + 1}"} }
    dest = []
    expected = source
    assert_result("--in-batches --batch-size 1", source, dest, expected)
  end

  def test_existing_data
    source = 3.times.map { |i| {"id" => i + 1, "title" => "Post #{i + 1}"} }
    dest = [{"id" => 1, "title" => "First Post"}, {"id" => 4, "title" => "Post 4"}]
    expected = dest
    assert_result("--in-batches --batch-size 1", source, dest, expected)
  end

  def test_overwrite
    assert_error "Cannot use --overwrite with --in-batches", "posts --in-batches --overwrite", config: true
  end

  def test_multiple_tables
    assert_error "Cannot use --in-batches with multiple tables", "--in-batches", config: true
  end
end
