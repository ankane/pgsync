module PgSync
  module Utils
    def log(message = nil)
      output.puts message
    end

    def colorize(message, color_code)
      if output.tty?
        "\e[#{color_code}m#{message}\e[0m"
      else
        message
      end
    end

    def output
      $stderr
    end

    def config_file
      return @config_file if instance_variable_defined?(:@config_file)

      @config_file =
        search_tree(
          if @options[:db]
            db_config_file(@options[:db])
          else
            @options[:config] || ".pgsync.yml"
          end
        )
    end

    def db_config_file(db)
      return unless db
      ".pgsync-#{db}.yml"
    end

    def search_tree(file)
      path = Dir.pwd
      # prevent infinite loop
      20.times do
        absolute_file = File.join(path, file)
        if File.exist?(absolute_file)
          break absolute_file
        end
        path = File.dirname(path)
        break if path == "/"
      end
    end
  end
end
