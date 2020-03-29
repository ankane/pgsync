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
      search_tree(db_config_file(@options[:db]) || @options[:config] || ".pgsync.yml")
    end

    def db_config_file(db)
      ".pgsync-#{db}.yml" if db
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
