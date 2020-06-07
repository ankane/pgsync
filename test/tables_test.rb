require_relative "test_helper"

class TablesTest < Minitest::Test
  def test_all
    tables = list_tables
    assert_includes tables, "posts"
    assert_includes tables, "other.pets"
    refute_includes tables, "excluded"
  end

  def test_wildcard
    tables = list_tables("public.*")
    assert_includes tables, "posts"
    refute_includes tables, "other.pets"
    refute_includes tables, "excluded"
  end

  def test_schemas
    tables = list_tables("--schemas public")
    assert_includes tables, "posts"
    refute_includes tables, "other.pets"
    refute_includes tables, "excluded"
  end

  def test_schemas_wildcard
    tables = list_tables("--schemas public p*")
    assert_includes tables, "posts"
    refute_includes tables, "other.pets"
  end

  def test_exclude_overrides_config
    tables = list_tables("--exclude posts")
    refute_includes tables, "posts"
    assert_includes tables, "excluded"
  end

  def test_exclude_not_applied_to_groups
    tables = list_tables("group_with_excluded")
    assert_includes tables, "excluded"
  end

  def list_tables(command = "")
    output = assert_works("--list #{command}", config: true)
    output.split("\n")[2..-1]
  end
end
