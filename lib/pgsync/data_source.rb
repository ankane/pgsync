module PgSync
  class DataSource
    include Utils

    attr_reader :url

    def initialize(url, name:, debug:)
      @url = url
      @name = name
      @debug = debug
    end

    def exists?
      @url && @url.size > 0
    end

    def local?
      !host || %w(localhost 127.0.0.1).include?(host)
    end

    def host
      @host ||= dedup_localhost(conninfo[:host])
    end

    def port
      @port ||= dedup_localhost(conninfo[:port])
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

    def max_id(table, primary_key, sql_clause = nil)
      execute("SELECT MAX(#{quote_ident(primary_key)}) FROM #{quote_ident_full(table)}#{sql_clause}").first["max"].to_i
    end

    def min_id(table, primary_key, sql_clause = nil)
      execute("SELECT MIN(#{quote_ident(primary_key)}) FROM #{quote_ident_full(table)}#{sql_clause}").first["min"].to_i
    end

    def last_value(seq)
      execute("SELECT last_value FROM #{quote_ident_full(seq)}").first["last_value"]
    end

    def truncate(table)
      execute("TRUNCATE #{quote_ident_full(table)} CASCADE")
    end

    def schemas
      @schemas ||= begin
        query = <<~SQL
          SELECT
            schema_name
          FROM
            information_schema.schemata
          ORDER BY 1
        SQL
        execute(query).map { |row| row["schema_name"] }
      end
    end

    def create_schema(schema)
      execute("CREATE SCHEMA #{quote_ident(schema)}")
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
      log_sql query, params
      conn.exec_params(query, params).to_a
    end

    def transaction
      if conn.transaction_status == 0
        # not currently in transaction
        log_sql "BEGIN"
        result =
          conn.transaction do
            yield
          end
        log_sql "COMMIT"
        result
      else
        yield
      end
    end

    # TODO log time for each statement
    def log_sql(query, params = {})
      if @debug
        message = "#{colorize("[#{@name}]", :cyan)} #{query.gsub(/\s+/, " ").strip}"
        message = "#{message} #{params.inspect}" if params.any?
        log message
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

    # for pg 1.4.4
    # https://github.com/ged/ruby-pg/issues/490
    def dedup_localhost(value)
      if conninfo[:host] == "localhost,localhost" && conninfo[:port].to_s.split(",").uniq.size == 1
        value.split(",")[0]
      else
        value
      end
    end
  end
end
