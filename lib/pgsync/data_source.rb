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
      %w(localhost 127.0.0.1).include?(uri.host)
    end

    def uri
      @uri ||= begin
        uri = URI.parse(@url)
        uri.scheme ||= "postgres"
        uri.host ||= "localhost"
        uri.port ||= 5432
        uri.path = "/#{uri.path}" if uri.path && uri.path[0] != "/"
        uri
      end
    end

    def schema
      @schema ||= CGI.parse(uri.query.to_s)["schema"][0] || "public"
    end

    def tables
      query = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = $1 ORDER BY tablename ASC"
      execute(query, [schema]).map { |row| row["tablename"] }
    end

    def table_exists?(table)
      query = "SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2"
      execute(query, [schema, table]).size > 0
    end

    def close
      if @conn
        conn.close
        @conn = nil
      end
    end

    def to_url
      uri = self.uri.dup
      uri.query = nil
      uri.to_s
    end

    def columns(table)
      query = "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2"
      execute(query, [schema, table]).map { |row| row["column_name"] }
    end

    def sequences(table, columns)
      execute("SELECT #{columns.map { |f| "pg_get_serial_sequence(#{escape("#{quote_ident(schema)}.#{quote_ident(table)}")}, #{escape(f)}) AS #{f}" }.join(", ")}")[0].values.compact
    end

    def max_id(table, primary_key, sql_clause = nil)
      execute("SELECT MAX(#{quote_ident(primary_key)}) FROM #{quote_ident(table)}#{sql_clause}")[0]["max"].to_i
    end

    def min_id(table, primary_key, sql_clause = nil)
      execute("SELECT MIN(#{quote_ident(primary_key)}) FROM #{quote_ident(table)}#{sql_clause}")[0]["min"].to_i
    end

    def last_value(seq)
      execute("select last_value from #{seq}")[0]["last_value"]
    end

    def truncate(table)
      execute("TRUNCATE #{quote_ident(table)} CASCADE")
    end

    # http://stackoverflow.com/a/20537829
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
      row = execute(query, [schema, quote_ident(table)])[0]
      row && row["attname"]
    end

    # borrowed from
    # ActiveRecord::ConnectionAdapters::ConnectionSpecification::ConnectionUrlResolver
    def conn
      @conn ||= begin
        begin
          uri_parser = URI::Parser.new
          config = {
              host: uri.host,
              port: uri.port,
              dbname: uri.path.sub(/\A\//, ""),
              user: uri.user,
              password: uri.password,
              connect_timeout: 3
          }.reject { |_, value| value.to_s.empty? }
          config.map { |key, value| config[key] = uri_parser.unescape(value) if value.is_a?(String) }
          conn = PG::Connection.new(config)
        rescue PG::ConnectionBad => e
          log
          raise PgSync::Error, e.message
        end
      end
    end

    private

    def execute(query, params = [])
      conn.exec_params(query, params).to_a
    end

    def log(message = nil)
      $stderr.puts message
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
      if source && source[0..1] == "$(" && source[-1] == ")"
        command = source[2..-2]
        source = `#{command}`.chomp
        unless $?.success?
          raise PgSync::Error, "Command exited with non-zero status:\n#{command}"
        end
      end
      source
    end
  end
end
