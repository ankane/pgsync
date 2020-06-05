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

    def warning(message)
      log colorize(message, :yellow)
    end

    def deprecated(message)
      warning "[DEPRECATED] #{message}"
    end

    def output
      $stderr
    end

    def config_file
      if @options[:config]
        @options[:config]
      elsif @options[:db]
        file = db_config_file(@options[:db])
        search_tree(file) || file
      else
        search_tree(".pgsync.yml")
      end
    end

    def db_config_file(db)
      ".pgsync-#{db}.yml"
    end

    def search_tree(file)
      return file if File.exist?(file)

      path = Dir.pwd
      # prevent infinite loop
      20.times do
        absolute_file = File.join(path, file)
        break absolute_file if File.exist?(absolute_file)
        path = File.dirname(path)
        break if path == "/"
      end
    end

    def quote_ident_full(ident)
      ident.split(".").map { |v| quote_ident(v) }.join(".")
    end

    def quote_ident(value)
      PG::Connection.quote_ident(value)
    end

    def escape(value)
      if value.is_a?(String)
        "'#{quote_string(value)}'"
      else
        value
      end
    end

    # activerecord
    def quote_string(s)
      s.gsub(/\\/, '\&\&').gsub(/'/, "''")
    end
  end
end
