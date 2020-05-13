require_relative "test_helper"

class InitTest < Minitest::Test
  def test_works
    new_dir do
      assert_works "--init"
      assert_match "?sslmode=require", File.read(".pgsync.yml")
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
