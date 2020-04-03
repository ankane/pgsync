module PgSync
  module Utils
    COLOR_CODES = {
      red: 31,
      green: 32,
      yellow: 33
    }

    def log(message = nil)
      output.puts message
    end

    def colorize(message, color)
      if output.tty?
        "\e[#{COLOR_CODES[color]}m#{message}\e[0m"
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
      return file if File.exists?(file)

      path = Dir.pwd
      # prevent infinite loop
      20.times do
        absolute_file = File.join(path, file)
        break absolute_file if File.exist?(absolute_file)
        path = File.dirname(path)
        break if path == "/"
      end
    end
  end
end
