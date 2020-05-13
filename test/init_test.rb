require_relative "test_helper"

class InitTest < Minitest::Test
  def test_works
    Dir.chdir(Dir.mktmpdir) do
      assert_works "--init"
      assert File.exist?(".pgsync.yml")
      assert_match /sslmode=require/, File.read(".pgsync.yml")
    end
  end
end
