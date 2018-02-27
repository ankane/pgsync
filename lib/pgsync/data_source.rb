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
      @schema ||= CGI.parse(uri.query.to_s)["schema"][0]
    end

    def tables
      query = "SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname NOT IN ('information_schema', 'pg_catalog') ORDER BY 1, 2"
      execute(query).map { |row| "#{row["schemaname"]}.#{row["tablename"]}" }
    end

    def table_exists?(table)
      query = "SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2"
      execute(query, table.split(".", 2)).size > 0
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
      execute(query, table.split(".", 2)).map { |row| row["column_name"] }
    end

    def sequences(table, columns)
      execute("SELECT #{columns.map { |f| "pg_get_serial_sequence(#{escape("#{quote_ident_full(table)}")}, #{escape(f)}) AS #{f}" }.join(", ")}")[0].values.compact
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
      row = execute(query, [table.split(".", 2)[0], quote_ident_full(table)])[0]
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

    def dump_command(tables)
      tables = tables.keys.map { |t| "-t #{Shellwords.escape(quote_ident_full(t))}" }.join(" ")
      dump_command = "pg_dump -Fc --verbose --schema-only --no-owner --no-acl #{tables} #{to_url}"
    end

    def restore_command
      psql_version = Gem::Version.new(`psql --version`.lines[0].chomp.split(" ")[-1].sub(/beta\d/, ""))
      if_exists = psql_version >= Gem::Version.new("9.4.0")
      restore_command = "pg_restore --verbose --no-owner --no-acl --clean #{if_exists ? "--if-exists" : nil} -d #{to_url}"
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
      execute("SELECT current_schemas(true)")[0]["current_schemas"][1..-2].split(",")
    end

    private

    def quote_ident_full(ident)
      ident.split(".", 2).map { |v| quote_ident(v) }.join(".")
    end

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
