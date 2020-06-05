module PgSync
  class DataSource
    include Utils

    attr_reader :url

    def initialize(url)
      @url = url
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
        query = <<~SQL
          SELECT
            table_schema AS schema,
            table_name AS table
          FROM
            information_schema.tables
          WHERE
            table_type = 'BASE TABLE' AND
            table_schema NOT IN ('information_schema', 'pg_catalog')
          ORDER BY 1, 2
        SQL
        execute(query).map { |row| Table.new(row["schema"], row["table"]) }
      end
    end

    def table_exists?(table)
      table_set.include?(table)
    end

    def sequences(table, columns)
      execute("SELECT #{columns.map { |f| "pg_get_serial_sequence(#{escape("#{quote_ident_full(table)}")}, #{escape(f)}) AS #{quote_ident(f)}" }.join(", ")}").first.values.compact
    end

    def max_id(table, primary_key, sql_clause = nil)
      execute("SELECT MAX(#{quote_ident(primary_key)}) FROM #{quote_ident_full(table)}#{sql_clause}").first["max"].to_i
    end

    def min_id(table, primary_key, sql_clause = nil)
      execute("SELECT MIN(#{quote_ident(primary_key)}) FROM #{quote_ident_full(table)}#{sql_clause}").first["min"].to_i
    end

    # this value comes from pg_get_serial_sequence which is already quoted
    def last_value(seq)
      execute("SELECT last_value FROM #{seq}").first["last_value"]
    end

    def truncate(table)
      execute("TRUNCATE #{quote_ident_full(table)} CASCADE")
    end

    # https://stackoverflow.com/a/20537829
    # TODO can simplify with array_position in Postgres 9.5+
    def primary_key(table)
      query = <<~SQL
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
      rows = execute(query, [table.schema, table.name])
      rows.sort_by { |r| r["indkey"].split(" ").index(r["attnum"]) }.map { |r| r["attname"] }
    end

    def triggers(table)
      query = <<~SQL
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
          @concurrent_id = concurrent_id
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

    # reconnect for new thread or process
    def reconnect_if_needed
      reconnect if @concurrent_id != concurrent_id
    end

    def search_path
      @search_path ||= execute("SELECT unnest(current_schemas(true)) AS schema").map { |r| r["schema"] }
    end

    def server_version_num
      @server_version_num ||= execute("SHOW server_version_num").first["server_version_num"].to_i
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

    private

    def concurrent_id
      [Process.pid, Thread.current.object_id]
    end

    def reconnect
      @conn.reset
      @concurrent_id = concurrent_id
    end

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
  end
end
