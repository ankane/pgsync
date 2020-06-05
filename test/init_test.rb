require_relative "test_helper"

class InitTest < Minitest::Test
  def test_works
    new_dir do
      assert_works "--init"
      assert_match "?sslmode=require", File.read(".pgsync.yml")
    end
  end

  def test_too_many_arguments
    assert_error "Usage:", "--init arg1 arg2"
  end

  def test_db_argument
    new_dir do
      assert_works "--init db2"
      assert File.exist?(".pgsync-db2.yml")
    end
  end

  def test_db_option
    new_dir do
      assert_works "--init --db db2"
      assert File.exist?(".pgsync-db2.yml")
    end
  end

  def test_config
    new_dir do
      assert_works "--init --config hi.yml"
      assert File.exist?("hi.yml")
    end
  end

  def test_rails
    new_dir do
      Dir.mkdir("bin")
      File.write("bin/rails", "")
      assert_works "--init"
      assert_match "schema_migrations", File.read(".pgsync.yml")
    end
  end

  def test_heroku
    new_dir do
      system "git init --quiet"
      system "git remote add heroku https://git.heroku.com/test.git"
      assert_works "--init"
      assert_match "$(heroku config:get DATABASE_URL)?sslmode=require", File.read(".pgsync.yml")
    end
  end

  def new_dir
    Dir.chdir(Dir.mktmpdir) do
      yield
    end
  end
end
