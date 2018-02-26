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
      conn.exec_params(query, [schema]).to_a.map { |row| row["tablename"] }
    end

    def table_exists?(table)
      query = "SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2"
      conn.exec_params(query, [schema, table]).to_a.size > 0
    end

    def close
      conn.close if conn
      @conn = nil
    end

    def to_url
      uri = uri.dup
      uri.query = nil
      uri.to_s
    end

    def columns(table)
      query = "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2"
      conn.exec_params(query, [schema, table]).to_a.map { |row| row["column_name"] }
    end

    def sequences(table, columns)
      conn.exec("SELECT #{columns.map { |f| "pg_get_serial_sequence(#{escape(quote_ident(table))}, #{escape(f)}) AS #{f}" }.join(", ")}").to_a[0].values.compact
    end

    def max_id(table, primary_key, sql_clause = nil)
      conn.exec("SELECT MAX(#{quote_ident(primary_key)}) FROM #{quote_ident(table)}#{sql_clause}").to_a[0]["max"].to_i
    end

    def min_id(table, primary_key, sql_clause = nil)
      conn.exec("SELECT MIN(#{quote_ident(primary_key)}) FROM #{quote_ident(table)}#{sql_clause}").to_a[0]["min"].to_i
    end

    def last_value(seq)
     conn.exec("select last_value from #{seq}").to_a[0]["last_value"]
    end

    def truncate(table)
      conn.exec("TRUNCATE #{quote_ident(table)} CASCADE")
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
      row = conn.exec_params(query, [schema, quote_ident(table)]).to_a[0]
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
