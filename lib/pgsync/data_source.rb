module PgSync
  class DataSource
    attr_reader :url

    def initialize(source, timeout: 3)
      @url = resolve_url(source)
      @timeout = timeout
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
    def primary_key(table)
      query = <<-SQL
        SELECT
          pg_attribute.attname,
          format_type(pg_attribute.atttypid, pg_attribute.atttypmod)
        FROM
          pg_index, pg_class, pg_attribute, pg_namespace
        WHERE
          pg_class.oid = $2::regclass AND
          indrelid = pg_class.oid AND
          nspname = $1 AND
          pg_class.relnamespace = pg_namespace.oid AND
          pg_attribute.attrelid = pg_class.oid AND
          pg_attribute.attnum = any(pg_index.indkey) AND
          indisprimary
      SQL
      row = execute(query, [table.split(".", 2)[0], quote_ident_full(table)])[0]
      row && row["attname"]
    end

    def conn
      @conn ||= begin
        begin
          ENV["PGCONNECT_TIMEOUT"] ||= @timeout.to_s
          if @url =~ /\Apostgres(ql)?:\/\//
            config = @url
          else
            config = {dbname: @url}
          end
          PG::Connection.new(config)
        rescue URI::InvalidURIError
          raise Error, "Invalid connection string"
        end
      end
    end

    def close
      if @conn
        @conn.close
        @conn = nil
      end
    end

    def dump_command(tables)
      tables = tables ? tables.keys.map { |t| "-t #{Shellwords.escape(quote_ident_full(t))}" }.join(" ") : ""
      "pg_dump -Fc --verbose --schema-only --no-owner --no-acl #{tables} -d #{@url}"
    end

    def restore_command
      if_exists = Gem::Version.new(pg_restore_version) >= Gem::Version.new("9.4.0")
      "pg_restore --verbose --no-owner --no-acl --clean #{if_exists ? "--if-exists" : nil} -d #{@url}"
    end

    def fully_resolve_tables(tables)
      no_schema_tables = {}
      search_path_index = Hash[search_path.map.with_index.to_a]
      self.tables.group_by { |t| t.split(".", 2)[-1] }.each do |group, t2|
        no_schema_tables[group] = t2.sort_by { |t| [search_path_index[t.split(".", 2)[0]] || 1000000, t] }[0]
      end

      Hash[tables.map { |k, v| [no_schema_tables[k] || k, v] }]
    end

    def search_path
      @search_path ||= execute("SELECT current_schemas(true)")[0]["current_schemas"][1..-2].split(",")
    end

    private

    def pg_restore_version
      `pg_restore --version`.lines[0].chomp.split(" ")[-1].split(/[^\d.]/)[0]
    rescue Errno::ENOENT
      raise Error, "pg_restore not found"
    end

    def table_set
      @table_set ||= Set.new(tables)
    end

    def conninfo
      @conninfo ||= conn.conninfo_hash
    end

    def quote_ident_full(ident)
      ident.split(".", 2).map { |v| quote_ident(v) }.join(".")
    end

    def execute(query, params = [])
      conn.exec_params(query, params).to_a
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
