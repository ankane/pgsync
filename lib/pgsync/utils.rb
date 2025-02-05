module PgSync
  module Utils
    COLOR_CODES = {
      red: 31,
      green: 32,
      yellow: 33,
      cyan: 36
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

    def db_config_file(db)
      ".pgsync-#{db}.yml"
    end

    def confirm_tables_exist(data_source, tasks, description)
      if description == "source"
        tasks.map(&:table).each do |table|
          unless data_source.table_exists?(table)
            raise Error, "Table not found in #{description}: #{table}"
          end
        end
      else
        tasks.map(&:destination_table).each do |table|
          unless data_source.table_exists?(table)
            raise Error, "Table not found in #{description}: #{table}"
          end
        end
      end
    end

    def first_schema
      @first_schema ||= source.search_path.find { |sp| sp != "pg_catalog" }
    end

    def task_name(task)
      friendly_name(task.table)
    end

    def friendly_name(table)
      if table.schema == first_schema
        table.name
      else
        table.full_name
      end
    end

    def quote_ident_full(ident)
      if ident.is_a?(Table) || ident.is_a?(Sequence)
        [quote_ident(ident.schema), quote_ident(ident.name)].join(".")
      else # temp table names are strings
        quote_ident(ident)
      end
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

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end
