module PgSync
  class DataSource
    attr_reader :url

    def initialize(source)
      @url = resolve_url(source)
    end

    def exists?
      @url && @url.size > 0
    end

    def local?
      !host || %w(localhost 127.0.0.1).include?(host)
    end

    def host
      @host ||= conninfo[:host]
    end

    def port
      @port ||= conninfo[:port]
    end

    def dbname
      @dbname ||= conninfo[:dbname]
    end

    # gets visible tables
    def tables
      @tables ||= begin
        query = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('information_schema', 'pg_catalog') ORDER BY 1, 2"
        execute(query).map { |row| "#{row["table_schema"]}.#{row["table_name"]}" }
      end
    end

    def table_exists?(table)
      table_set.include?(table)
    end

    def columns(table)
      query = "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2"
      execute(query, table.split(".", 2)).map { |row| row["column_name"] }
    end

    def sequences(table, columns)
      execute("SELECT #{columns.map { |f| "pg_get_serial_sequence(#{escape("#{quote_ident_full(table)}")}, #{escape(f)}) AS #{quote_ident(f)}" }.join(", ")}")[0].values.compact
    end

    def max_id(table, primary_key, sql_clause = nil)
      execute("SELECT MAX(#{quote_ident(primary_key)}) FROM #{quote_ident_full(table)}#{sql_clause}")[0]["max"].to_i
    end

    def min_id(table, primary_key, sql_clause = nil)
      execute("SELECT MIN(#{quote_ident(primary_key)}) FROM #{quote_ident_full(table)}#{sql_clause}")[0]["min"].to_i
    end

    def last_value(seq)
      execute("select last_value from #{seq}")[0]["last_value"]
    end

    def truncate(table)
      execute("TRUNCATE #{quote_ident_full(table)} CASCADE")
    end

    # https://stackoverflow.com/a/20537829
    # TODO can simplify with array_position in Postgres 9.5+
    def primary_key(table)
      query = <<-SQL
        SELECT
          pg_attribute.attname,
          format_type(pg_attribute.atttypid, pg_attribute.atttypmod),
          pg_attribute.attnum,
          pg_index.indkey
        FROM
          pg_index, pg_class, pg_attribute, pg_namespace
        WHERE
          nspname = $1 AND
          relname = $2 AND
          indrelid = pg_class.oid AND
          pg_class.relnamespace = pg_namespace.oid AND
          pg_attribute.attrelid = pg_class.oid AND
          pg_attribute.attnum = any(pg_index.indkey) AND
          indisprimary
      SQL
      rows = execute(query, table.split(".", 2))
      rows.sort_by { |r| r["indkey"].split(" ").index(r["attnum"]) }.map { |r| r["attname"] }
    end

    def triggers(table)
      query = <<-SQL
        SELECT
          tgname AS name,
          tgisinternal AS internal,
          tgenabled != 'D' AS enabled,
          tgconstraint != 0 AS integrity
        FROM
          pg_trigger
        WHERE
          pg_trigger.tgrelid = $1::regclass
      SQL
      execute(query, [quote_ident_full(table)])
    end

    def conn
      @conn ||= begin
        begin
          ENV["PGCONNECT_TIMEOUT"] ||= "3"
          if @url =~ /\Apostgres(ql)?:\/\//
            config = @url
          else
            config = {dbname: @url}
          end
          PG::Connection.new(config)
        rescue URI::InvalidURIError
          raise Error, "Invalid connection string. Make sure it works with `psql`"
        end
      end
    end

    def close
      if @conn
        @conn.close
        @conn = nil
      end
    end

    def reconnect
      @conn.reset
    end

    def search_path
      @search_path ||= execute("SELECT current_schemas(true)")[0]["current_schemas"][1..-2].split(",")
    end

    def execute(query, params = [])
      conn.exec_params(query, params).to_a
    end

    def transaction
      if conn.transaction_status == 0
        # not currently in transaction
        conn.transaction do
          yield
        end
      else
        yield
      end
    end

    def quote_ident_full(ident)
      ident.split(".", 2).map { |v| quote_ident(v) }.join(".")
    end

    def quote_ident(value)
      PG::Connection.quote_ident(value)
    end

    private

    def table_set
      @table_set ||= Set.new(tables)
    end

    def conninfo
      @conninfo ||= begin
        unless conn.respond_to?(:conninfo_hash)
          raise Error, "libpq is too old. Upgrade it and run `gem install pg`"
        end
        conn.conninfo_hash
      end
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

    def resolve_url(source)
      if source
        source = source.dup
        source.gsub!(/\$\([^)]+\)/) do |m|
          command = m[2..-2]
          result = `#{command}`.chomp
          unless $?.success?
            raise Error, "Command exited with non-zero status:\n#{command}"
          end
          result
        end
      end
      source
    end
  end
end
