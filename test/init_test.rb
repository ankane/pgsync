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

  def test_django
    new_dir do
      File.write("manage.py", "django")
      assert_works "--init"
      assert_excludes "django_migrations"
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

  def test_laravel
    new_dir do
      File.write("artisan", "")
      assert_works "--init"
      assert_excludes "migrations"
    end
  end

  def test_rails
    new_dir do
      Dir.mkdir("bin")
      File.write("bin/rails", "")
      assert_works "--init"
      assert_excludes "ar_internal_metadata"
      assert_excludes "schema_migrations"
    end
  end

  def new_dir
    Dir.chdir(Dir.mktmpdir) do
      yield
    end
  end

  def assert_excludes(table)
    assert_match "- #{table}", File.read(".pgsync.yml")
  end
end
