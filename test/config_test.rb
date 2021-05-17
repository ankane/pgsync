require_relative "test_helper"

class ConfigTest < Minitest::Test
    def test_process_erb
        with_temp_file(<<~EOF
            from: "pgsync_test1"
            to: <%= 'pgsync_test2' %>
        EOF
        ) do |path|
            assert_works "--config #{path}"
        end
    end

    def test_process_shell_command_in_config
        with_temp_file(<<~EOF
            from: "pgsync_test1"
            to: $(echo 'pgsync_test2')
        EOF
        ) do |path|
            assert_works "--config #{path}"
        end
    end

    def with_temp_file(content = '', &block)
        file = Tempfile.new('config')
        file.write(content)
        file.close
        block.call(file.path)
    ensure
        file.unlink
    end
end
